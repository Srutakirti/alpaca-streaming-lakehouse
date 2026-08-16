# Alpaca bar frame contract

Every producer publishes one UTF-8 JSON array per Kafka message. The array items use the Alpaca bar WebSocket fields in `alpaca-bar-frame.schema.json`. Python synthetic input, the external Fakepaca stream through WSR, and future real Alpaca input all share this contract.
