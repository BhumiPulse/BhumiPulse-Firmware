/**
 * @file network_layer.c
 * @brief Handles Mesh Routing, Retries, ACKs, Neighbor Tables, and Sequence filtering.
 * @author Shahid  
 * @date Feb 2026
 */

#include <stdio.h>
#include <string.h>
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_random.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "network_layer.h"
#include "mac_layer.h"
#include "farmpulse_config.h"

static const char *TAG = "NETWORK";
#define BROADCAST_ID 0xFF

// --- EmSave Thresholds ---
#define LORA_RSSI_THRESHOLD_BASE -120 // Our LoRa Thershoild
#define LORA_RSSI_HYSTERESIS     10

#ifdef CONFIG_BHUMIPULSE_ROLE_GATEWAY
extern bool alive_table[256];
#endif

neighbor_entry_t neighbor_table[NEIGHBOR_TABLE_SIZE];
static uint8_t current_seq_num = 1; // EmSave standard starts at 1

static SemaphoreHandle_t ack_wait_sem = NULL;
static SemaphoreHandle_t tx_pipeline_mutex = NULL; 
static uint8_t expected_ack_seq = 0;
static uint8_t expected_ack_from = 0;
static bool is_waiting_for_ack = false;

static network_receive_cb_t app_rx_cb = NULL;
static QueueHandle_t relay_queue = NULL;

void network_register_cb(network_receive_cb_t cb) { app_rx_cb = cb; }
static void mesh_relay_task(void *pvParameters);
static uint32_t millis() { return (uint32_t)(esp_timer_get_time() / 1000); }


void network_init(void) {
    ESP_LOGI(TAG, "Initializing EmSave Network Layer. My ID: %d", system_config.node_id);
    ack_wait_sem = xSemaphoreCreateBinary();
    tx_pipeline_mutex = xSemaphoreCreateMutex();
    
    for (int i = 0; i < NEIGHBOR_TABLE_SIZE; i++) {
        neighbor_table[i].status = NODE_DISCONNECTED;
        neighbor_table[i].node_id = 0xFF; 
        neighbor_table[i].rx_seq = 0; // Initialize sequence memory to 0
    }

    relay_queue = xQueueCreate(5, sizeof(farm_packet_t));
    xTaskCreate(mesh_relay_task, "mesh_relay", 4096, NULL, 6, NULL);
}


int find_neighbor_index(uint8_t id) {
    for (int i = 0; i < NEIGHBOR_TABLE_SIZE; i++) {
        if (neighbor_table[i].status == NODE_CONNECTED && neighbor_table[i].node_id == id) return i;
    }
    return -1;
}


void update_neighbor(uint8_t id, int8_t rssi, uint8_t rx_seq) {
    if (id == BROADCAST_ID) return; 

    // =================================================================
    // TEMPORARY BENCH TEST: FORCE MESH HOPPING THROUGH NODE 1
    // =================================================================
    // if (system_config.node_id == 0 && id == 2) return; 
    // if (system_config.node_id == 2 && id == 0) return; 
    // =================================================================

    int idx = find_neighbor_index(id);

    // 1. Threshold Evaluation: Only apply when considering a NEW device
    if (idx == -1) {
        int8_t required_threshold = -115; 
        if (id == 0) {
            required_threshold = -105; 
        }

        if (rssi < required_threshold) {
            return; // Signal too weak. Exit without adding.
        }

        for (int i = 0; i < NEIGHBOR_TABLE_SIZE; i++) {
            if (neighbor_table[i].status == NODE_DISCONNECTED) {
                neighbor_table[i].node_id = id;
                neighbor_table[i].status = NODE_CONNECTED;
                neighbor_table[i].rssi = rssi;
                neighbor_table[i].last_seen_ms = millis();
                neighbor_table[i].rx_seq = rx_seq; // CRITICAL: Initialize sequence ONLY ONCE
                ESP_LOGW(TAG, "*** New Neighbor Linked: Node %d (RSSI %d) ***", id, rssi);
                break;
            }
        }
    } else {
        // 2. Update stats for EXISTING neighbors
        neighbor_table[idx].status = NODE_CONNECTED;
        neighbor_table[idx].rssi = rssi;
        neighbor_table[idx].last_seen_ms = millis();
        
        // ABSOLUTELY DO NOT OVERWRITE rx_seq HERE! 
        // Updating it here destroys the duplicate rejection logic in network_handle_packet.
    }
}


void get_neighbour_ids(uint8_t *buffer, uint8_t *count) {
    // NOte: This function only provide the higher Ids of the neighbor table from myaddr. The lower IDs are not included in this list.
    uint8_t idx = 0;
    for (int i = 0; i < NEIGHBOR_TABLE_SIZE; i++) {
        if (neighbor_table[i].node_id > system_config.node_id && neighbor_table[i].status == NODE_CONNECTED) {
            buffer[idx++] = neighbor_table[i].node_id;
        }
    }
    *count = idx;
}


// --- TRUE EMSAVE HUNTING ALGORITHM ---
static uint8_t get_next_hop(uint8_t final_dest) {
    if (final_dest == BROADCAST_ID) return BROADCAST_ID;

    // 1. Context 1: Direct Connection Priority
    int direct_idx = find_neighbor_index(final_dest);
    if (direct_idx != -1 && neighbor_table[direct_idx].status == NODE_CONNECTED) {
        return final_dest; 
    }

    // 2. Context 3: Linear Hopping
    uint8_t best_candidate = 0xFF;

    if (final_dest > system_config.node_id) { 
        // Upstream: Find HIGHEST neighbor ID that is <= Destination
        uint8_t highest_valid = 0;
        bool found = false;
        for (int i = 0; i < NEIGHBOR_TABLE_SIZE; i++) {
            if (neighbor_table[i].status == NODE_CONNECTED) {
                uint8_t nid = neighbor_table[i].node_id;
                if (nid > system_config.node_id && nid <= final_dest) {
                    if (!found || nid > highest_valid) {
                        highest_valid = nid;
                        best_candidate = nid;
                        found = true;
                    }
                }
            }
        }
    } else { 
        // Downstream: Find LOWEST neighbor ID that is >= Destination
        uint8_t lowest_valid = 0xFF;
        bool found = false;
        for (int i = 0; i < NEIGHBOR_TABLE_SIZE; i++) {
            if (neighbor_table[i].status == NODE_CONNECTED) {
                uint8_t nid = neighbor_table[i].node_id;
                if (nid < system_config.node_id && nid >= final_dest) {
                    if (!found || nid < lowest_valid) {
                        lowest_valid = nid;
                        best_candidate = nid;
                        found = true;
                    }
                }
            }
        }
    }
    
    return best_candidate;
}


void send_ack(uint8_t target_node, uint8_t acked_seq_num) {
    farm_packet_t ack_pkt;
    ack_pkt.header.target_id = target_node; 
    ack_pkt.header.sender_id = system_config.node_id;
    ack_pkt.header.network_id = system_config.network_id;
    ack_pkt.header.seq_num = current_seq_num++; 
    ack_pkt.header.fcf = PKT_TYPE_ACK; 
    ack_pkt.header.hop_count = 1; 
    ack_pkt.header.final_dest_id = target_node;
    ack_pkt.header.origin_src_id = system_config.node_id;
    ack_pkt.payload[0] = acked_seq_num;
    ack_pkt.header.length = 10 + 1; 
    mac_tx(&ack_pkt);
}


void network_handle_packet(farm_packet_t *pkt, int8_t rssi) {

    // 1. MRFI (MAC) LAYER HDR EXTRACTION
    uint8_t mrfiDestAddr = pkt->header.target_id;
    uint8_t mrfiSrcAddr = pkt->header.sender_id;
    uint8_t rxSeqNum = pkt->header.seq_num;

    // 2. NWK (NETWORK) LAYER HDR EXTRACTION
    uint8_t NwkDestAddr = pkt->header.final_dest_id;
    uint8_t NwkSrcAddr = pkt->header.origin_src_id;
    uint8_t ucMsgType = (pkt->header.fcf & 0x0F);

    // ============================================================================
    // 3. OVERHEARD PACKET FILTER (CRITICAL ECHO-STORM FIX)
    // ============================================================================
    // If this packet is NOT addressed to my MAC ID, I must learn the route and SILENTLY DROP IT.
    if (mrfiDestAddr != system_config.node_id && mrfiDestAddr != BROADCAST_ID)
    {
        if (mrfiSrcAddr != BROADCAST_ID)
        {
            update_neighbor(mrfiSrcAddr, rssi, rxSeqNum);
        }
        ESP_LOGW(TAG, "Dropped overheard packet meant for Node %d", mrfiDestAddr);
        return; // ABSOLUTELY CRITICAL: Do not send ACKs, do not execute, do not relay!
    }

    // 4. NEIGHBOR DISCOVERY (For packets actually addressed to us)
    if (mrfiSrcAddr != BROADCAST_ID)
    {
        update_neighbor(mrfiSrcAddr, rssi, rxSeqNum);
    }

    // 5. HANDLE INCOMING MAC-ACKs
    if (ucMsgType == PKT_TYPE_ACK)
    {
        ESP_LOGI(TAG, ">> ACK RECEIVED from Node %d for Seq: %d", mrfiSrcAddr, pkt->payload[0]);
        if (is_waiting_for_ack && mrfiSrcAddr == expected_ack_from && pkt->payload[0] == expected_ack_seq)
        {
            xSemaphoreGive(ack_wait_sem);
        }
        return;
    }

    // 6. STRICT SEQUENCE VALIDATION (DUPLICATE REJECTION)
    int idx = find_neighbor_index(mrfiSrcAddr);
    if (idx != -1 && mrfiDestAddr != BROADCAST_ID)
    {
        if (rxSeqNum == 0 || rxSeqNum != neighbor_table[idx].rx_seq)
        {
            neighbor_table[idx].rx_seq = rxSeqNum;
        }
        else
        {
            ESP_LOGW(TAG, "MESH LOOP AVOIDED: Duplicate frame rejected (Seq: %d, From: %d)",
                     rxSeqNum, mrfiSrcAddr);
            if (pkt->header.fcf & FCF_MASK_ACK_REQ)
            {
                send_ack(mrfiSrcAddr, rxSeqNum);
            }
            return;
        }
    }

    // 7. SEND IMMEDIATE HOP-BY-HOP ACK (If Requested)
    if ((pkt->header.fcf & FCF_MASK_ACK_REQ) && mrfiDestAddr != BROADCAST_ID)
    {
        send_ack(mrfiSrcAddr, rxSeqNum);
    }

    // ============================================================================
    // 8. NWK LAYER PROCESSING: EXECUTE vs. RELAY
    // ============================================================================

    if (NwkDestAddr == BROADCAST_ID || ucMsgType == PKT_TYPE_BROADCAST)
    {
        if (app_rx_cb != NULL)
        {
            app_rx_cb(NwkSrcAddr, ucMsgType, pkt->payload, pkt->header.length - 10);
        }
    }
    else if (system_config.node_id == NwkDestAddr)
    {
        // IsForMe: The frame has reached its ultimate destination.
        ESP_LOGD(TAG, "Packet Accepted (Type: 0x%02X)", ucMsgType);
        if (app_rx_cb != NULL)
        {
            app_rx_cb(NwkSrcAddr, ucMsgType, pkt->payload, pkt->header.length - 10);
        }
    }
    else
    {
        // NotForMe: I am an intermediary node. Act as a Mesh Relay.

        // CRITICAL LOOP PREVENTION: Never relay a packet that I originated!
        if (NwkSrcAddr == system_config.node_id)
        {
            ESP_LOGW(TAG, "Ghost Loop Blocked: Packet originated from me. Dropping.");
            return;
        }

        if (pkt->header.hop_count > 0)
        {
            uint8_t next_hop = get_next_hop(NwkDestAddr);

            if (next_hop != 0xFF)
            {
                ESP_LOGW(TAG, "=================================================");
                ESP_LOGW(TAG, "  INTERCEPTED PACKET - ACTING AS MESH RELAY");
                ESP_LOGW(TAG, "  Origin: Node %d  --->  Final Dest: Node %d", NwkSrcAddr, NwkDestAddr);
                ESP_LOGW(TAG, "  Routing through Next Hop: Node %d", next_hop);
                ESP_LOGW(TAG, "=================================================");

                farm_packet_t relay_pkt;
                memcpy(&relay_pkt, pkt, sizeof(farm_packet_t));
                relay_pkt.header.target_id = next_hop;
                relay_pkt.header.sender_id = system_config.node_id;
                relay_pkt.header.seq_num = current_seq_num++;
                relay_pkt.header.hop_count--;

                if (xQueueSend(relay_queue, &relay_pkt, 0) != pdTRUE)
                {
                    ESP_LOGE(TAG, "Relay Queue Full! Dropping packet for %d", NwkDestAddr);
                }
            }
        }
        else
        {
            ESP_LOGE(TAG, "Relay Dropped: No route to %d", NwkDestAddr);
        }
    }
}

// ============================================================================
// ASYNCHRONOUS MESH RELAY TASK (Prevents RX Deadlocks)
// ============================================================================
static void mesh_relay_task(void *pvParameters) {
    farm_packet_t relay_pkt;
    
    while (1) {
        // Wait indefinitely until a packet is pushed into the queue
        if (xQueueReceive(relay_queue, &relay_pkt, portMAX_DELAY) == pdTRUE) {
            bool relay_success = false;
            uint8_t retries = CONFIG_MESH_MAX_RETRIES;
            bool requires_ack = (relay_pkt.header.fcf & FCF_MASK_ACK_REQ);
            uint8_t next_hop = relay_pkt.header.target_id;

            if (requires_ack) {
                expected_ack_seq = relay_pkt.header.seq_num;
                expected_ack_from = next_hop;
                is_waiting_for_ack = true;
                xSemaphoreTake(ack_wait_sem, 0); 
            }

            while (retries > 0) {
                mac_tx(&relay_pkt); 
                if (!requires_ack) { relay_success = true; break; }

                if (xSemaphoreTake(ack_wait_sem, pdMS_TO_TICKS(6000)) == pdTRUE) {
                    relay_success = true;
                    break;
                } else {
                    retries--;
                    if (retries > 0) {
                        ESP_LOGW(TAG, "Relay: No ACK from Node %d. Retrying...", next_hop);
                        // OPTIMIZATION 2: Add a small random jitter to prevent RF collisions
                        vTaskDelay(pdMS_TO_TICKS(100 + (esp_random() % 200)));
                    }
                }
            }

            is_waiting_for_ack = false;
            if (!relay_success) {
                ESP_LOGE(TAG, "Relay Failed: Route to %d collapsed.", next_hop);
                int failed_idx = find_neighbor_index(next_hop);
                if (failed_idx != -1) neighbor_table[failed_idx].status = NODE_DISCONNECTED;
                
                #ifdef CONFIG_BHUMIPULSE_ROLE_GATEWAY
                alive_table[next_hop] = false;
                #endif
            }
        }
    }
}


bool network_send(uint8_t dest_id, packet_type_t type, uint8_t *payload, uint8_t len) {
    xSemaphoreTake(tx_pipeline_mutex, portMAX_DELAY);
    
    farm_packet_t pkt;
    uint8_t next_hop;

    if (dest_id == BROADCAST_ID) {
        next_hop = BROADCAST_ID;
    } else {
        next_hop = get_next_hop(dest_id);
        
        if (next_hop == 0xFF) {
            ESP_LOGE(TAG, "Cannot Send: No Route to %d", dest_id);
            xSemaphoreGive(tx_pipeline_mutex);
            return false;
        }
    }

    pkt.header.length = 10 + len;
    pkt.header.target_id = next_hop;
    //pkt.header.target_id = next_hop;
    pkt.header.sender_id = system_config.node_id;
    pkt.header.network_id = system_config.network_id;
    pkt.header.seq_num = current_seq_num++;
    
    pkt.header.fcf = (type & 0x0F);
    if (dest_id != BROADCAST_ID && type != PKT_TYPE_ACK) {
        pkt.header.fcf |= FCF_MASK_ACK_REQ; 
    }

    pkt.header.hop_count = CONFIG_MESH_MAX_HOPS; 
    pkt.header.final_dest_id = dest_id;
    pkt.header.origin_src_id = system_config.node_id;
    memcpy(pkt.payload, payload, len);

    bool requires_ack = (pkt.header.fcf & FCF_MASK_ACK_REQ);
    bool tx_success = false;
    uint8_t retries = CONFIG_MESH_MAX_RETRIES;

    if (requires_ack) {
        expected_ack_seq = pkt.header.seq_num;
        expected_ack_from = next_hop;
        is_waiting_for_ack = true;
        xSemaphoreTake(ack_wait_sem, 0); 
    }

    while (retries > 0) {
        mac_tx(&pkt);
        if (!requires_ack) { tx_success = true; break; }

        if (xSemaphoreTake(ack_wait_sem, pdMS_TO_TICKS(6000)) == pdTRUE) {
            tx_success = true;
            break;
        } else {
            retries--;
            if (retries > 0) ESP_LOGW(TAG, "No ACK from Node %d. Retrying...", next_hop);
        }
    }

    is_waiting_for_ack = false;
    if (!tx_success && requires_ack) {
        int idx = find_neighbor_index(next_hop);
        if (idx != -1) neighbor_table[idx].status = NODE_DISCONNECTED;
        // NOTE: In the next step, we will notify the Gateway here as per Declare_Dead.md
    }

    xSemaphoreGive(tx_pipeline_mutex);
    return tx_success;
}