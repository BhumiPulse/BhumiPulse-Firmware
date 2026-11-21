# BhumiPulse Firmware Monorepo

Official embedded firmware repository for BhumiPulse Industrial IoT Hardware (Gateway & Field Nodes).

## Build Environment (Pinned for Reproducibility)
- **MCU Target:** ESP32-S3 (LilyGO T3S3 / BhumiPulse Production PCB)
- **Framework:** ESP-IDF v5.5.2
- **Structure:**
  - `apps/Farm_Gateway`: Gateway application (`Wi-Fi / EG800 Cellular + MQTT + LoRa Master`)
  - `apps/Farm_Node`: Field Node application (`3-Phase ZMPT/ADS1115 + Sensors + Motor Relay + LoRa Mesh`)
  - `shared_components/`: Single source of truth for LoRa RF stack, protocol definitions, NVS config, and shared drivers.