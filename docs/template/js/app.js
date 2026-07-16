
    const schema = {
  "asyncapi": "3.1.0",
  "info": {
    "title": "Gesture Broadcast API",
    "version": "1.0.0",
    "description": "API do przesyłania danych o gestach przez WebSockets."
  },
  "servers": {
    "local": {
      "host": "localhost:8080",
      "protocol": "ws"
    }
  },
  "channels": {
    "gestureChannel": {
      "address": "/ws",
      "summary": "Główny kanał komunikacji dla gestów.",
      "messages": {
        "gestureUpdate": {
          "name": "GestureUpdate",
          "title": "Aktualizacja gestu",
          "payload": {
            "type": "object",
            "properties": {
              "angles": {
                "type": "array",
                "items": {
                  "type": "number",
                  "minimum": 0,
                  "maximum": 180,
                  "x-parser-schema-id": "<anonymous-schema-2>"
                },
                "minItems": 6,
                "maxItems": 6,
                "x-parser-schema-id": "<anonymous-schema-1>"
              },
              "timestamp": {
                "type": "number",
                "description": "Unix timestamp (float64)",
                "x-parser-schema-id": "<anonymous-schema-3>"
              }
            },
            "required": [
              "angles",
              "timestamp"
            ],
            "x-parser-schema-id": "GestureData"
          },
          "x-parser-unique-object-id": "gestureUpdate"
        }
      },
      "x-parser-unique-object-id": "gestureChannel"
    }
  },
  "operations": {
    "sendGesture": {
      "action": "send",
      "channel": "$ref:$.channels.gestureChannel",
      "summary": "Klient wysyła dane o gestach (6 kątów).",
      "x-parser-unique-object-id": "sendGesture"
    },
    "receiveBroadcast": {
      "action": "receive",
      "channel": "$ref:$.channels.gestureChannel",
      "summary": "Serwer rozsyła dane do wszystkich podłączonych klientów.",
      "x-parser-unique-object-id": "receiveBroadcast"
    }
  },
  "components": {
    "messages": {
      "GestureUpdate": "$ref:$.channels.gestureChannel.messages.gestureUpdate"
    },
    "schemas": {
      "GestureData": "$ref:$.channels.gestureChannel.messages.gestureUpdate.payload"
    }
  },
  "x-parser-spec-parsed": true,
  "x-parser-api-version": 3,
  "x-parser-spec-stringified": true
};
    const config = {"show":{"sidebar":true},"sidebar":{"showOperations":"byDefault"}};
    const appRoot = document.getElementById('root');
    AsyncApiStandalone.render(
        { schema, config, }, appRoot
    );
  