#define FIRMWARE_VERSION "1.3"


#include <Wire.h>
#include <RTClib.h>
#include <WiFiUdp.h>
#include <ESPping.h>
#include <LittleFS.h>
#include <NTPClient.h>
#include <ArduinoJson.h>
#include <PZEM004Tv30.h>
#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <Adafruit_GFX.h>
#include <SoftwareSerial.h>
#include <Adafruit_SSD1306.h>
#include <ESP8266WebServer.h>


#if defined(ESP32)
#error "Software Serial is not supported on the ESP32"
#endif


#if !defined(PZEM_RX_PIN) && !defined(PZEM_TX_PIN)
#define PZEM_RX_PIN D6
#define PZEM_TX_PIN D5
#endif


#define BUTTON_PIN D7


SoftwareSerial pzemSWSerial(PZEM_RX_PIN, PZEM_TX_PIN);
PZEM004Tv30 pzem(pzemSWSerial);


// Generate Device ID dari Chip ID
const char* DEVICE_ID = []() -> const char* {
  static char idStr[7];
  uint64_t chipId;


#ifdef ESP8266
  chipId = ESP.getChipId();
#elif defined(ESP32)
  chipId = ESP.getEfuseMac();
#endif


  const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  const int len = sizeof(alphanum) - 1;


  for (int i = 0; i < 6; i++) {
    idStr[i] = alphanum[(chipId >> (i * 5)) % len];
  }
  idStr[6] = '\0';


  return idStr;
}();


// ==================== CONFIG STRUCTURE ====================
#define CONFIG_PATH "/config.json"


struct Conf {
  bool BOOT_TO_SETUP;
  char WIFI_SSID[32];
  char WIFI_PASS[64];
  char MQTT_SERVER[64];
  uint16_t MQTT_PORT;
  char MQTT_USER[32];
  char USERNAME[32];  // Username dari app
  char DEVICE_NAME[50];
  char DEVICE_LOCATION[50];
  char MQTT_OUT_RAW[64];
  char MQTT_IN_CMD[64];
  char MQTT_OUT_STATUS[64];
  uint16_t REPORT_INTERVAL;  // Interval kirim data (detik)
  bool AUTO_SYNC_RTC;
  int8_t TIMEZONE_OFFSET;
  uint16_t RTC_SYNC_INTERVAL;  // Interval auto sync RTC (jam)
  bool PREVENT_ZERO_READING;
};


Conf conf = {
  .BOOT_TO_SETUP = false,
  .MQTT_PORT = 1883,
  .REPORT_INTERVAL = 1,
  .AUTO_SYNC_RTC = true,
  .TIMEZONE_OFFSET = 7,
  .RTC_SYNC_INTERVAL = 24,  // Default 24 jam
  .PREVENT_ZERO_READING = true
};


// ==================== SETUP MODE ====================
ESP8266WebServer server(80);
bool setupMode = false;


// ==================== HARDWARE ====================
IPAddress ping_ip(8, 8, 8, 8);


#define SCREEN_WIDTH 128
#define SCREEN_HEIGHT 64
Adafruit_SSD1306 display(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, -1);
RTC_DS3231 rtc;


WiFiUDP ntpUDP;
NTPClient timeClient(ntpUDP, "id.pool.ntp.org", 0, 60000);  // Offset akan diset nanti


WiFiClient espClient;
PubSubClient client(espClient);


// ==================== VARIABLES ====================
bool isRTCLostPower = false;
bool isRTCValid = false;  // Flag untuk tracking status RTC
bool isBoot = true;
bool bootState = true;
bool buttonState = false;
unsigned long button_time = 0;
unsigned long lastUpdateMillis = 0;
unsigned long lastRTCSync = 0;  // Untuk periodic sync
unsigned long time_2000 = 0;
unsigned long time_250 = 0;
unsigned long mqtt_rto = 0;
unsigned long lastPublish = 0;
byte internet = 0;
byte syncDelay = 0;


// Sensor data
float voltage = 0;
float current = 0;
float power = 0;
float energy = 0;
float frequency = 0;
float pf = 0;


// Display menu
const char* menu[] = { "Back", "Device Info", "Sync RTC", "Setup Mode", "Factory Reset", "Restart Device" };
const byte menuTotal = 6;
byte screenPage = 0;
byte screenIndex = 0;


// ==================== FORWARD DECLARATIONS ====================
void startSetupMode();
void handleConfigPost();
void handleStatus();
void handleWiFiScan();
bool saveConfig();
bool loadConfig();
String fsRead(String path);
bool fsWrite(String path, String data);
bool syncRTC();
void checkPeriodicRTCSync();
byte internetCheck();
bool readPzem();
void updateWiFiSignal();
void refreshDisplay();
void callback(char* topic, byte* payload, unsigned int length);
void reconnect();
void buttonPress();
void onButtonShort();
void onButtonLong();


// ==================== SETUP ====================
void setup() {
  Serial.begin(115200);
  Serial.println();
  Serial.println("=== SiWatt Meter Starting ===");
  Serial.printf("Firmware Version: %s\n", FIRMWARE_VERSION);


  Wire.begin();
  pinMode(BUTTON_PIN, INPUT_PULLUP);
  attachInterrupt(BUTTON_PIN, buttonPress, FALLING);


  // OLED init
  if (!display.begin(SSD1306_SWITCHCAPVCC, 0x3C)) {
    Serial.println(F("OLED gagal!"));
    while (1)
      ;
  }


  display.setTextColor(SSD1306_WHITE);
  display.clearDisplay();
  display.setTextSize(2);
  display.setCursor(64 - (5 * 2 * 4), 16);
  display.print("Si Watt");
  display.setTextSize(1);
  display.setCursor(64 - (5 * 6), 48);
  display.print("Smart Meter");
  display.display();
  delay(1000);


  display.clearDisplay();
  display.setCursor(0, 0);
  display.print("Device ID:");
  display.setCursor(0, 12);
  display.print(DEVICE_ID);
  display.display();
  delay(1000);


  // RTC init
  display.clearDisplay();
  display.setCursor(0, 0);
  display.print("Begin RTC");
  display.display();


  if (!rtc.begin()) {
    Serial.println("RTC tidak ditemukan");
    display.println(" FAIL");
    display.display();
    while (1)
      ;
  } else {
    display.println(" OK");
  }


  // Check RTC power loss
  isRTCLostPower = rtc.lostPower();

  if (isRTCLostPower) {
    Serial.println("WARNING: RTC lost power! Need NTP sync.");
    isRTCValid = false;
  } else {
    Serial.println("RTC power OK");
    isRTCValid = true;

    // Validasi waktu RTC (cek apakah masuk akal)
    DateTime now = rtc.now();
    if (now.year() < 2020 || now.year() > 2100) {
      Serial.println("WARNING: RTC time invalid! Need NTP sync.");
      isRTCValid = false;
    }
  }


  // LittleFS init
  display.print("Init Storage");
  display.display();


  if (!LittleFS.begin()) {
    Serial.println("LittleFS Mount Failed, formatting...");
    LittleFS.format();
    if (!LittleFS.begin()) {
      display.println(" FAIL");
      display.display();
      while (1)
        ;
    }
  }
  display.println(" OK");
  display.display();


  // Load config
  display.print("Loading Config");
  display.display();
  Serial.println("Loading conf...");


  if (!loadConfig()) {
    Serial.println("No config found, entering setup mode");
    display.println(" FAIL");
    display.println("Setup Mode...");
    display.display();
    delay(1000);


    startSetupMode();
    return;  // Stop here, setup mode handles loop
  }


  display.println(" OK");
  display.display();
  delay(500);


  if (conf.BOOT_TO_SETUP) {
    display.println("Booting to Setup...");
    display.display();
    delay(1000);
    startSetupMode();
    return;
  }


  // WiFi connect
  display.println("Connect WiFi...");
  display.display();
  Serial.println("Connecting WiFi...");


  WiFi.mode(WIFI_STA);
  WiFi.hostname(String("SiWatt-") + DEVICE_ID);
  WiFi.begin(conf.WIFI_SSID, conf.WIFI_PASS);


  uint8_t timeout = 30;
  while (WiFi.status() != WL_CONNECTED && timeout > 0) {
    delay(500);
    Serial.print(".");
    timeout--;
  }


  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\nWiFi connected: " + WiFi.localIP().toString());
    display.println("WiFi OK");
  } else {
    Serial.println("\nWiFi failed!");
    display.println("WiFi FAIL");
  }
  display.display();
  delay(500);


  // MQTT setup
  randomSeed(micros());
  display.println("Setup MQTT...");
  display.display();
  Serial.println("Set MQTT...");


  client.setServer(conf.MQTT_SERVER, conf.MQTT_PORT);
  client.setCallback(callback);


  // NTP setup dengan timezone offset configurable
  timeClient.begin();
  timeClient.setTimeOffset(conf.TIMEZONE_OFFSET * 3600);
  Serial.printf("NTP timezone offset: %d hours\n", conf.TIMEZONE_OFFSET);


  // Sync RTC if needed
  if (conf.AUTO_SYNC_RTC && !isRTCValid && WiFi.status() == WL_CONNECTED) {
    display.println("Sync RTC...");
    display.display();

    if (syncRTC()) {
      display.println("RTC Synced OK");
      isRTCValid = true;
      isRTCLostPower = false;
      isBoot = false;  // Skip boot sequence
      lastRTCSync = millis();
    } else {
      display.println("RTC Sync Failed");
    }
    display.display();
    delay(1000);
  } else if (isRTCValid) {
    isBoot = false;  // RTC sudah valid, skip boot sequence
    lastRTCSync = millis();
  }


  display.clearDisplay();
  display.drawLine(0, 9, 128, 9, WHITE);
  display.display();


  Serial.println("=== Setup Complete ===");
  Serial.printf("RTC Status: %s\n", isRTCValid ? "Valid" : "Invalid");
  Serial.printf("Boot Mode: %s\n", isBoot ? "Active" : "Skipped");
}


// ==================== MAIN LOOP ====================
void loop() {
  unsigned long now = millis();


  // Handle setup mode
  if (setupMode) {
    server.handleClient();


    if (conf.BOOT_TO_SETUP) {
      if (buttonState) {
        if (digitalRead(BUTTON_PIN) == HIGH) {
          buttonState = false;
          if (now - button_time >= 1500) {
          }
        } else if (now - button_time >= 1500) {
          buttonState = false;
          conf.BOOT_TO_SETUP = false;
          saveConfig();
          ESP.restart();
        }
      }
    }
    return;
  }


  // Button handling
  if (buttonState) {
    if (digitalRead(BUTTON_PIN) == HIGH) {
      buttonState = false;
      if (now - button_time >= 1500) {
        onButtonLong();
      } else {
        onButtonShort();
      }
    } else {
      if (now - button_time >= 1500) {
        buttonState = false;
        onButtonLong();
      }
    }
  }


  // MQTT reconnect
  if (!client.connected()) {
    if (now - mqtt_rto >= 5000) {
      mqtt_rto = now;
      if (internet == 3) reconnect();
    }
  }


  // Blink indicator
  if (now - time_250 >= 250) {
    time_250 = now;
    if (isBoot || !client.connected()) {
      if (bootState) display.fillRect(80, 4, 4, 4, WHITE);
      else display.fillRect(80, 4, 4, 4, BLACK);
      display.display();
      bootState = !bootState;
    } else if (!bootState) {
      display.fillRect(80, 4, 4, 4, BLACK);
      display.display();
    }
  }


  // Update & publish sensor data
  if (now - lastUpdateMillis >= 1000) {
    lastUpdateMillis = now;
    internet = internetCheck();


    // Boot sync RTC - hanya jika RTC tidak valid
    if (isBoot) {
      if (internet == 3 && !isRTCValid) {
        if (timeClient.update()) {
          if (timeClient.isTimeSet()) {
            syncDelay++;
            Serial.printf("Boot sync delay: %d/5\n", syncDelay);

            if (syncDelay >= 5) {
              if (syncRTC()) {
                Serial.println("Boot RTC sync successful");
                isRTCValid = true;
                isRTCLostPower = false;
                isBoot = false;
                lastRTCSync = now;
              } else {
                Serial.println("Boot RTC sync failed, retrying...");
                syncDelay = 0;  // Reset untuk retry
              }
            }
          }
        } else {
          Serial.println("NTP update failed during boot");
        }
      } else if (isRTCValid) {
        // RTC sudah valid, keluar dari boot mode
        Serial.println("RTC valid, exiting boot mode");
        isBoot = false;
        lastRTCSync = now;
      } else if (internet < 3) {
        Serial.println("Waiting for internet connection...");
      }
    }


    // Publish sensor data sesuai interval - hanya jika RTC valid
    if (!isBoot && isRTCValid) {
      DateTime rtc_now = rtc.now();
      char waktu[9];
      sprintf(waktu, "%02d:%02d:%02d", rtc_now.hour(), rtc_now.minute(), rtc_now.second());
      char tanggal[11];
      sprintf(tanggal, "%02d-%02d-%04d", rtc_now.day(), rtc_now.month(), rtc_now.year());


      if (now - lastPublish >= (conf.REPORT_INTERVAL * 1000)) {
        lastPublish = now;

        if (readPzem() && client.connected()) {
          JsonDocument doc;
          doc["device_id"] = DEVICE_ID;
          doc["datetime"] = String(tanggal) + " " + String(waktu);
          doc["voltage"] = roundf(voltage * 100) / 100.0;
          doc["current"] = roundf(current * 1000) / 1000.0;
          doc["power"] = roundf(power * 10) / 10.0;
          doc["energy"] = roundf(energy * 1000) / 1000.0;
          doc["frequency"] = roundf(frequency * 100) / 100.0;
          doc["pf"] = roundf(pf * 100) / 100.0;
          doc["rssi"] = WiFi.RSSI();
          doc["uptime"] = millis() / 1000;


          String data;
          serializeJson(doc, data);
          client.publish(conf.MQTT_OUT_RAW, data.c_str());
          Serial.println("Published: " + data);
        }
      }


      // Update display time
      if (screenPage == 0) {
        display.fillRect(0, 0, 6 * 10, 8, BLACK);
        display.setCursor(0, 0);
        display.print(waktu);
        display.display();
      }

      // Check periodic RTC sync
      checkPeriodicRTCSync();

    } else if (!isRTCValid) {
      // RTC tidak valid, tampilkan error
      if (screenPage == 0) {
        display.fillRect(0, 0, 6 * 10, 8, BLACK);
        display.setCursor(0, 0);
        display.print("Time Error");
        display.display();
      }
    }


    refreshDisplay();
  }


  // Update WiFi signal
  if (now - time_2000 >= 2000) {
    time_2000 = now;
    updateWiFiSignal();
  }


  client.loop();
  if (WiFi.status() == WL_CONNECTED) {
    timeClient.update();
  }
}


// ==================== SETUP MODE FUNCTIONS ====================
void startSetupMode() {
  setupMode = true;


  WiFi.mode(WIFI_AP);
  String apSSID = "SiWatt-" + String(DEVICE_ID);
  WiFi.softAP(apSSID.c_str(), "12345678");


  Serial.println("===== SETUP MODE =====");
  Serial.println("SSID: " + apSSID);
  Serial.println("Pass: 12345678");
  Serial.println("IP: " + WiFi.softAPIP().toString());
  Serial.println("======================");


  // Web server endpoints
  server.on("/status", HTTP_GET, handleStatus);
  server.on("/scan", HTTP_GET, handleWiFiScan);
  server.on("/config", HTTP_POST, handleConfigPost);
  server.onNotFound([]() {
    server.send(404, "application/json", "{\"error\":\"Not found\"}");
  });


  server.begin();


  // Display
  display.clearDisplay();
  display.setTextSize(1);
  display.setCursor(0, 0);
  display.println("=== SETUP MODE ===");
  display.println();
  display.println("SSID:");
  display.setTextSize(1);
  display.println(apSSID);
  display.println("Pass: 12345678");
  if (conf.BOOT_TO_SETUP) display.println("\nLong press to cancel");


  display.display();
}


void handleStatus() {
  JsonDocument doc;
  doc["device_id"] = DEVICE_ID;
  doc["mode"] = "setup";
  doc["version"] = FIRMWARE_VERSION;
  doc["chip_id"] = ESP.getChipId();
  doc["free_heap"] = ESP.getFreeHeap();
  doc["uptime"] = millis() / 1000;


  String response;
  serializeJson(doc, response);
  server.send(200, "application/json", response);
}


void handleWiFiScan() {
  Serial.println("Scanning WiFi...");
  int n = WiFi.scanNetworks();


  JsonDocument doc;
  JsonArray networks = doc["networks"].to<JsonArray>();


  for (int i = 0; i < n; i++) {
    JsonObject net = networks.add<JsonObject>();
    net["ssid"] = WiFi.SSID(i);
    net["rssi"] = WiFi.RSSI(i);
    net["secure"] = (WiFi.encryptionType(i) != ENC_TYPE_NONE);
  }


  doc["count"] = n;


  String response;
  serializeJson(doc, response);
  server.send(200, "application/json", response);
}


void handleConfigPost() {
  if (!server.hasArg("plain")) {
    server.send(400, "application/json", "{\"error\":\"No body\"}");
    return;
  }


  String body = server.arg("plain");
  Serial.println("Received config: " + body);


  JsonDocument doc;
  DeserializationError error = deserializeJson(doc, body);


  if (error) {
    Serial.println("JSON parse error");
    server.send(400, "application/json", "{\"error\":\"Invalid JSON\"}");
    return;
  }


  // Validate required fields
  if (!doc.containsKey("wifi_ssid") || !doc.containsKey("wifi_pass") || !doc.containsKey("username")) {
    server.send(400, "application/json", "{\"error\":\"Missing required fields\"}");
    return;
  }


  // Update config
  conf.BOOT_TO_SETUP = false;
  strncpy(conf.WIFI_SSID, doc["wifi_ssid"] | "", sizeof(conf.WIFI_SSID));
  strncpy(conf.WIFI_PASS, doc["wifi_pass"] | "", sizeof(conf.WIFI_PASS));
  strncpy(conf.MQTT_SERVER, doc["mqtt_server"] | "broker.emqx.io", sizeof(conf.MQTT_SERVER));
  conf.MQTT_PORT = doc["mqtt_port"] | 1883;
  strncpy(conf.MQTT_USER, doc["mqtt_user"] | "", sizeof(conf.MQTT_USER));
  strncpy(conf.USERNAME, doc["username"] | "user", sizeof(conf.USERNAME));
  strncpy(conf.DEVICE_NAME, doc["device_name"] | "SiWatt Meter", sizeof(conf.DEVICE_NAME));
  strncpy(conf.DEVICE_LOCATION, doc["device_location"] | "", sizeof(conf.DEVICE_LOCATION));
  conf.REPORT_INTERVAL = doc["report_interval"] | 1;
  conf.TIMEZONE_OFFSET = doc["timezone_offset"] | 7;
  conf.RTC_SYNC_INTERVAL = doc["rtc_sync_interval"] | 24;


  // Generate MQTT topics dengan username
  snprintf(conf.MQTT_OUT_RAW, sizeof(conf.MQTT_OUT_RAW), "/siwatt-mqtt/%s/swm-raw/%s", conf.USERNAME, DEVICE_ID);
  snprintf(conf.MQTT_IN_CMD, sizeof(conf.MQTT_IN_CMD), "/siwatt-mqtt/%s/swm-cmd/%s", conf.USERNAME, DEVICE_ID);
  snprintf(conf.MQTT_OUT_STATUS, sizeof(conf.MQTT_OUT_STATUS), "/siwatt-mqtt/%s/swm-status/%s", conf.USERNAME, DEVICE_ID);


  Serial.println("Config updated:");
  Serial.println("WiFi SSID: " + String(conf.WIFI_SSID));
  Serial.println("MQTT Server: " + String(conf.MQTT_SERVER));
  Serial.println("Username: " + String(conf.USERNAME));
  Serial.println("MQTT Topic: " + String(conf.MQTT_OUT_RAW));
  Serial.printf("Timezone: UTC+%d\n", conf.TIMEZONE_OFFSET);


  // Save config
  if (saveConfig()) {
    Serial.println("Config saved successfully");
    server.send(200, "application/json", "{\"status\":\"success\",\"message\":\"Device will restart\"}");
    delay(1000);
    ESP.restart();
  } else {
    Serial.println("Failed to save config");
    server.send(500, "application/json", "{\"error\":\"Failed to save config\"}");
  }
}


// ==================== CONFIG FUNCTIONS ====================
bool saveConfig() {
  JsonDocument doc;
  doc["boot_to_setup"] = conf.BOOT_TO_SETUP;
  doc["wifi_ssid"] = conf.WIFI_SSID;
  doc["wifi_pass"] = conf.WIFI_PASS;
  doc["mqtt_server"] = conf.MQTT_SERVER;
  doc["mqtt_port"] = conf.MQTT_PORT;
  doc["mqtt_user"] = conf.MQTT_USER;
  doc["username"] = conf.USERNAME;
  doc["device_name"] = conf.DEVICE_NAME;
  doc["device_location"] = conf.DEVICE_LOCATION;
  doc["mqtt_out_raw"] = conf.MQTT_OUT_RAW;
  doc["mqtt_in_cmd"] = conf.MQTT_IN_CMD;
  doc["mqtt_out_status"] = conf.MQTT_OUT_STATUS;
  doc["report_interval"] = conf.REPORT_INTERVAL;
  doc["auto_sync_rtc"] = conf.AUTO_SYNC_RTC;
  doc["timezone_offset"] = conf.TIMEZONE_OFFSET;
  doc["rtc_sync_interval"] = conf.RTC_SYNC_INTERVAL;
  doc["prevent_zero_reading"] = conf.PREVENT_ZERO_READING;


  String confData;
  serializeJson(doc, confData);


  Serial.println("Saving config: " + confData);
  return fsWrite(CONFIG_PATH, confData);
}


bool loadConfig() {
  String file = fsRead(CONFIG_PATH);
  if (file.length() == 0) {
    Serial.println("Config file empty or not found");
    return false;
  }


  JsonDocument doc;
  DeserializationError error = deserializeJson(doc, file);
  if (error) {
    Serial.println("Failed to parse config: " + String(error.c_str()));
    return false;
  }


  conf.BOOT_TO_SETUP = doc["boot_to_setup"] | false;
  strncpy(conf.WIFI_SSID, doc["wifi_ssid"] | "", sizeof(conf.WIFI_SSID));
  strncpy(conf.WIFI_PASS, doc["wifi_pass"] | "", sizeof(conf.WIFI_PASS));
  strncpy(conf.MQTT_SERVER, doc["mqtt_server"] | "broker.emqx.io", sizeof(conf.MQTT_SERVER));
  conf.MQTT_PORT = doc["mqtt_port"] | 1883;
  strncpy(conf.MQTT_USER, doc["mqtt_user"] | "", sizeof(conf.MQTT_USER));
  strncpy(conf.USERNAME, doc["username"] | "user", sizeof(conf.USERNAME));
  strncpy(conf.DEVICE_NAME, doc["device_name"] | "SiWatt Meter", sizeof(conf.DEVICE_NAME));
  strncpy(conf.DEVICE_LOCATION, doc["device_location"] | "", sizeof(conf.DEVICE_LOCATION));
  strncpy(conf.MQTT_OUT_RAW, doc["mqtt_out_raw"] | "", sizeof(conf.MQTT_OUT_RAW));
  strncpy(conf.MQTT_IN_CMD, doc["mqtt_in_cmd"] | "", sizeof(conf.MQTT_IN_CMD));
  strncpy(conf.MQTT_OUT_STATUS, doc["mqtt_out_status"] | "", sizeof(conf.MQTT_OUT_STATUS));
  conf.REPORT_INTERVAL = doc["report_interval"] | 1;
  conf.AUTO_SYNC_RTC = doc["auto_sync_rtc"] | true;
  conf.TIMEZONE_OFFSET = doc["timezone_offset"] | 7;
  conf.RTC_SYNC_INTERVAL = doc["rtc_sync_interval"] | 24;
  conf.PREVENT_ZERO_READING = doc["prevent_zero_reading"];


  // Validate - kalau SSID kosong, config invalid
  if (strlen(conf.WIFI_SSID) == 0) {
    Serial.println("Config invalid: empty SSID");
    return false;
  }


  Serial.println("Config loaded successfully");
  Serial.println("WiFi SSID: " + String(conf.WIFI_SSID));
  Serial.println("MQTT Server: " + String(conf.MQTT_SERVER));
  Serial.println("Username: " + String(conf.USERNAME));
  Serial.printf("Timezone: UTC+%d\n", conf.TIMEZONE_OFFSET);
  Serial.printf("RTC Sync Interval: %d hours\n", conf.RTC_SYNC_INTERVAL);


  return true;
}


String fsRead(String path) {
  if (!LittleFS.exists(path)) {
    return "";
  }


  File file = LittleFS.open(path, "r");
  if (!file) {
    Serial.println("Failed to open file for reading");
    return "";
  }


  String data = "";
  while (file.available()) {
    data += char(file.read());
  }
  file.close();
  return data;
}


bool fsWrite(String path, String data) {
  File file = LittleFS.open(path, "w");
  if (!file) {
    Serial.println("Failed to open file for writing");
    return false;
  }


  size_t written = file.print(data);
  file.close();


  return written == data.length();
}


// ==================== MQTT FUNCTIONS ====================
void callback(char* topic, byte* payload, unsigned int length) {
  String data;
  for (unsigned int i = 0; i < length; i++) {
    data += (char)payload[i];
  }


  Serial.printf("MQTT MSG [%s]: %s\n", topic, data.c_str());


  JsonDocument doc;
  DeserializationError error = deserializeJson(doc, data);


  if (!error && doc.containsKey("cmd")) {
    String cmd = doc["cmd"].as<String>();


    if (cmd == "pzem-reset") {
      pzem.resetEnergy();
      Serial.println("PZEM energy reset");
      client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"PZEM energy reset\"}");

    } else if (cmd == "reboot") {
      Serial.println("Rebooting...");
      client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"Rebooting\"}");
      delay(500);
      ESP.restart();

    } else if (cmd == "sync-rtc") {
      if (syncRTC()) {
        client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"RTC synced successfully\"}");
      } else {
        client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"RTC sync failed\"}");
      }

    } else if (cmd == "factory-reset") {
      LittleFS.remove(CONFIG_PATH);
      client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"Factory reset\"}");
      delay(500);
      ESP.restart();

    } else if (cmd == "set-interval") {
      conf.REPORT_INTERVAL = doc["value"] | 1;
      saveConfig();
      client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"Interval updated\"}");

    } else if (cmd == "set-timezone") {
      conf.TIMEZONE_OFFSET = doc["value"] | 7;
      timeClient.setTimeOffset(conf.TIMEZONE_OFFSET * 3600);
      saveConfig();

      JsonDocument response;
      response["status"] = "Timezone updated";
      response["timezone"] = conf.TIMEZONE_OFFSET;
      String responseStr;
      serializeJson(response, responseStr);
      client.publish(conf.MQTT_OUT_STATUS, responseStr.c_str());

    } else if (cmd == "get-info") {
      JsonDocument info;
      info["device_id"] = DEVICE_ID;
      info["device_name"] = conf.DEVICE_NAME;
      info["location"] = conf.DEVICE_LOCATION;
      info["firmware"] = FIRMWARE_VERSION;
      info["uptime"] = millis() / 1000;
      info["rssi"] = WiFi.RSSI();
      info["ip"] = WiFi.localIP().toString();
      info["free_heap"] = ESP.getFreeHeap();
      info["report_interval"] = conf.REPORT_INTERVAL;
      info["timezone"] = conf.TIMEZONE_OFFSET;
      info["rtc_valid"] = isRTCValid;
      info["rtc_lost_power"] = isRTCLostPower;


      String response;
      serializeJson(info, response);
      client.publish(conf.MQTT_OUT_STATUS, response.c_str());
    } else if (cmd == "no-zero-read-enable") {
      conf.PREVENT_ZERO_READING = true;
      Serial.println("prevent zero read enabled");
      client.publish(conf.MQTT_OUT_STATUS, "{\"prevent_zero_read\":\"true\"}");
      saveConfig();
    } else if (cmd == "no-zero-read-disable") {
      conf.PREVENT_ZERO_READING = false;
      Serial.println("prevent zero read disabled");
      client.publish(conf.MQTT_OUT_STATUS, "{\"prevent_zero_read\":\"false\"}");
      saveConfig();
    }
  }
}


void reconnect() {
  Serial.print("Attempting MQTT connection...");


  String clientId = "SWM-" + String(DEVICE_ID) + "-" + String(random(0xffff), HEX);


  bool connected = false;
  if (strlen(conf.MQTT_USER) > 0) {
    // MQTT dengan auth
    connected = client.connect(clientId.c_str(), conf.MQTT_USER, "");
  } else {
    // MQTT tanpa auth
    connected = client.connect(clientId.c_str());
  }


  if (connected) {
    Serial.println("connected");
    client.subscribe(conf.MQTT_IN_CMD);


    // Publish online status
    JsonDocument doc;
    doc["device_id"] = DEVICE_ID;
    doc["status"] = "online";
    doc["uptime"] = millis() / 1000;
    doc["firmware"] = FIRMWARE_VERSION;
    doc["rtc_valid"] = isRTCValid;


    String status;
    serializeJson(doc, status);
    client.publish(conf.MQTT_OUT_STATUS, status.c_str(), true);
  } else {
    Serial.print("failed, rc=");
    Serial.println(client.state());
  }
}


// ==================== SENSOR FUNCTIONS ====================
bool readPzem() {
  float newVoltage = pzem.voltage();
  float newCurrent = pzem.current();
  float newPower = pzem.power();
  float newEnergy = pzem.energy();
  float newFrequency = pzem.frequency();
  float newPf = pzem.pf();
  bool error = false;

  if (conf.PREVENT_ZERO_READING && (newVoltage <= 0 || isnan(newVoltage))) {
    Serial.println("PZEM Read Error.");
    display.clearDisplay();
    display.setCursor(0, 0);
    display.printf("PZEM READ ERROR...\n");
    display.printf("%03.2f V\n", voltage);
    display.printf("%03.3f A\n", current);
    display.printf("%0.1f W\n", power);
    display.printf("%0.3f KwH\n", energy);
    display.printf("%02.2f Hz\n", frequency);
    display.printf("%0.2f  ", pf);
    display.printf("%s \n", DEVICE_ID);
    display.printf("Restarting...");

    delay(2000);
    ESP.restart();
  }

  if (isnan(newVoltage)) {
    newVoltage = 0;
    error = true;
  }
  if (isnan(newCurrent)) {
    newCurrent = 0;
    error = true;
  }
  if (isnan(newPower)) {
    newPower = 0;
    error = true;
  }
  if (isnan(newEnergy)) {
    newEnergy = 0;
    error = true;
  }
  if (isnan(newFrequency)) {
    newFrequency = 0;
    error = true;
  }
  if (isnan(newPf)) {
    newPf = 0;
    error = true;
  }


  voltage = newVoltage;
  current = newCurrent;
  frequency = newFrequency;
  power = newPower;
  energy = newEnergy;
  pf = newPf;


  return !error;
}


// ==================== RTC SYNC FUNCTIONS ====================
bool syncRTC() {
  if (internetCheck() < 2) {
    Serial.println("No internet connection for RTC sync");
    return false;
  }


  Serial.println("Syncing RTC with NTP...");


  // Retry mechanism
  int retries = 3;
  bool success = false;


  while (retries > 0 && !success) {
    // Force update NTP
    if (timeClient.forceUpdate()) {
      if (timeClient.isTimeSet()) {
        unsigned long epochTime = timeClient.getEpochTime();

        // Validasi epoch time (harus > 2020-01-01)
        if (epochTime > 1577836800) {  // 2020-01-01 00:00:00
          rtc.adjust(DateTime(epochTime));


          // Verify RTC was set correctly
          DateTime now = rtc.now();
          if (now.year() >= 2020 && now.year() <= 2100) {
            isRTCValid = true;
            isRTCLostPower = false;
            lastRTCSync = millis();


            Serial.println("RTC synced successfully");
            Serial.printf("New RTC time: %02d-%02d-%04d %02d:%02d:%02d\n",
                          now.day(), now.month(), now.year(),
                          now.hour(), now.minute(), now.second());
            success = true;
          } else {
            Serial.println("RTC verification failed - invalid time after sync");
          }
        } else {
          Serial.println("Invalid epoch time from NTP");
        }
      } else {
        Serial.println("NTP time not set");
      }
    } else {
      Serial.println("NTP update failed");
    }


    if (!success) {
      retries--;
      if (retries > 0) {
        Serial.printf("Retrying RTC sync... (%d attempts left)\n", retries);
        delay(2000);
      }
    }
  }


  if (!success) {
    Serial.println("RTC sync failed after all retries");
  }


  return success;
}


void checkPeriodicRTCSync() {
  // Auto sync RTC setiap interval yang ditentukan
  if (!conf.AUTO_SYNC_RTC || conf.RTC_SYNC_INTERVAL == 0) {
    return;
  }


  unsigned long syncInterval = (unsigned long)conf.RTC_SYNC_INTERVAL * 3600000;  // Jam ke milidetik
  unsigned long now = millis();


  // Handle millis overflow
  if (now < lastRTCSync) {
    lastRTCSync = now;
    return;
  }


  if (now - lastRTCSync >= syncInterval) {
    Serial.printf("Periodic RTC sync triggered (interval: %d hours)\n", conf.RTC_SYNC_INTERVAL);

    if (syncRTC()) {
      Serial.println("Periodic RTC sync successful");

      // Publish sync status ke MQTT
      if (client.connected()) {
        JsonDocument doc;
        doc["event"] = "rtc_synced";
        doc["type"] = "periodic";
        doc["timestamp"] = timeClient.getEpochTime();

        String msg;
        serializeJson(doc, msg);
        client.publish(conf.MQTT_OUT_STATUS, msg.c_str());
      }
    } else {
      Serial.println("Periodic RTC sync failed");
    }
  }
}


// ==================== DISPLAY FUNCTIONS ====================
void updateWiFiSignal() {
  int rssi = WiFi.RSSI();
  int bars = 0;


  if (rssi < -90) rssi = -90;
  if (rssi > -60) rssi = -60;
  bars = map(rssi, -90, -60, 0, 7);


  display.fillRect(128 - (8 + 6 * 5), 0, 8, 8, BLACK);
  for (int i = 0; i < bars; i++) {
    int h = (i + 1);
    display.fillRect(128 - (8 + 6 * 5) + i, 8 - h, 1, h, WHITE);
  }


  display.fillRect(128 - (6 * 5) + 3, 0, 6 * 5, 8, BLACK);
  display.setCursor(128 - (6 * 5) + 3, 0);


  if (internet == 0) display.print("X");
  else if (internet >= 2) display.printf("%02dms", Ping.maxTime());
  else display.print("- ms");


  display.display();
}


void refreshDisplay() {
  display.fillRect(0, 10, 128, 64 - 10, BLACK);


  switch (screenPage) {
    case 0:
      {
        switch (screenIndex) {
          case 0:
            {
              display.setCursor(0, 12);
              display.printf("%03.2f V\n", voltage);
              display.printf("%03.3f A\n", current);
              display.printf("%0.1f W\n", power);
              display.printf("%0.3f KwH\n", energy);
              display.printf("%02.2f Hz\n", frequency);
              display.printf("%0.2f  ", pf);
              display.printf("%s \n", DEVICE_ID);
              break;
            }
          case 1:
            {
              String text = String(voltage, 1) + " V";
              if (text.length() <= 7) {
                display.setTextSize(3);
                display.setCursor(0, 18);
              } else {
                display.setTextSize(2);
                display.setCursor(64 - 6 * 2 * ((text.length() + 1) / 2), 18);
              }
              display.print(text);
              display.setTextSize(1);
              display.setCursor(0, 64 - 16);
              display.printf("A:%03.3f A  P:%03.1f W\n", current, power);
              display.printf("F:%0.1f Hz  pf:%0.2f ", frequency, pf);
              break;
            }
          case 2:
            {
              String text = String(current, 3) + " A";
              if (text.length() <= 7) {
                display.setTextSize(3);
                display.setCursor(0, 18);
              } else {
                display.setTextSize(2);
                display.setCursor(64 - 6 * 2 * ((text.length() + 1) / 2), 18);
              }
              display.print(text);
              display.setTextSize(1);
              display.setCursor(0, 64 - 16);
              display.printf("V:%0.1f V  P:%03.1f W\n", voltage, power);
              display.printf("F:%0.1f Hz  pf:%0.2f ", frequency, pf);
              break;
            }
          case 3:
            {
              String text = String(power, 1) + " W";
              if (text.length() <= 7) {
                display.setTextSize(3);
                display.setCursor(0, 18);
              } else {
                display.setTextSize(2);
                display.setCursor(64 - 6 * 2 * ((text.length() + 1) / 2), 18);
              }
              display.print(text);
              display.setTextSize(1);
              display.setCursor(0, 64 - 16);
              display.printf("V:%0.1f V  A:%03.3f A\n", voltage, current);
              display.printf("F:%0.1f Hz  pf:%0.2f ", frequency, pf);
              break;
            }
          case 4:
            {
              String text = String(frequency, 2) + " Hz";
              if (text.length() <= 7) {
                display.setTextSize(3);
                display.setCursor(0, 18);
              } else {
                display.setTextSize(2);
                display.setCursor(64 - 6 * 2 * ((text.length() + 1) / 2), 18);
              }
              display.print(text);
              display.setTextSize(1);
              display.setCursor(0, 64 - 16);
              display.printf("V:%0.1f V  A:%03.3f A\n", voltage, current);
              display.printf("P:%0.1f W  pf:%0.2f ", power, pf);
              break;
            }
          case 5:
            {
              String text = "pf: " + String(pf, 2);
              if (text.length() <= 7) {
                display.setTextSize(3);
                display.setCursor(0, 18);
              } else {
                display.setTextSize(2);
                display.setCursor(64 - 6 * 2 * ((text.length() + 1) / 2), 18);
              }
              display.print(text);
              display.setTextSize(1);
              display.setCursor(0, 64 - 16);
              display.printf("V:%0.1f V  A:%03.3f A\n", voltage, current);
              display.printf("F:%0.1f Hz  P:%0.1f W", frequency, power);
              break;
            }
          default:
            {
              screenIndex = 0;
              refreshDisplay();
              break;
            }
        }
        break;
      }


    case 1:
      {
        if (screenIndex >= menuTotal) screenIndex = 0;


        display.fillRect(0, 0, 6 * 10, 8, BLACK);
        display.setCursor(0, 0);
        display.setTextSize(1);
        display.print("Menu");


        display.fillRect(0, 12, 8, 64 - 12, BLACK);
        display.fillRect(0, ((screenIndex * 7) + screenIndex) + 13, 4, 5, WHITE);


        for (byte i = 0; i < menuTotal; i++) {
          display.setCursor(8, 12 + 8 * i);
          display.printf("%s", menu[i]);
        }
        break;
      }


    case 2:
      {
        display.fillRect(0, 0, 6 * 10, 8, BLACK);
        display.setCursor(0, 0);
        display.setTextSize(1);
        display.print("Status");


        display.setCursor(0, 12);
        switch (screenIndex) {
          case 0:
            {
              unsigned long totalMili = millis();
              unsigned long totalDetik = totalMili / 1000;
              int detik = totalDetik % 60;
              unsigned long totalMenit = totalDetik / 60;
              int menit = totalMenit % 60;
              unsigned long totalJam = totalMenit / 60;
              int jam = totalJam % 24;
              int hari = totalJam / 24;


              display.printf("Owner: %s\n", conf.USERNAME);
              display.printf("Device ID: %s\n", DEVICE_ID);
              display.printf("Version: %s\n", FIRMWARE_VERSION);
              display.printf("Free Heap: %d Kb\n", ESP.getFreeHeap() / 1024);
              display.printf("UpTime: %02d:%02d:%02d:%02d\n", hari, jam, menit, detik);
              break;
            }
          case 1:
            {
              display.printf("SSID: %s\n", WiFi.SSID());
              display.printf("WiFi: %s\n", WiFi.status() == WL_CONNECTED ? "Connected" : "Disconnected");
              display.printf("IP: ");
              display.println(WiFi.localIP());
              display.printf("GW: ");
              display.println(WiFi.gatewayIP());
              display.printf("Signal: ");
              display.println(WiFi.RSSI());
              break;
            }
          case 2:
            {
              display.printf("Serv: %s\n", conf.MQTT_SERVER);
              display.printf("MQTT: %s\n", client.connected() ? "Connected" : "Disconnected");
              display.printf("Port: %d\n", conf.MQTT_PORT);
              display.printf("User: %s\n", conf.MQTT_USER);
              break;
            }
          case 3:
            {
              display.printf("RTC Valid: %s\n", isRTCValid ? "Yes" : "No");
              display.printf("Lost Power: %s\n", isRTCLostPower ? "Yes" : "No");
              display.printf("Timezone: UTC+%d\n", conf.TIMEZONE_OFFSET);
              display.printf("Sync Interval: %dh\n", conf.RTC_SYNC_INTERVAL);

              unsigned long nextSync = 0;
              if (lastRTCSync > 0) {
                unsigned long elapsed = millis() - lastRTCSync;
                unsigned long interval = (unsigned long)conf.RTC_SYNC_INTERVAL * 3600000;
                if (elapsed < interval) {
                  nextSync = (interval - elapsed) / 60000;  // Minutes
                }
              }
              display.printf("Next Sync: %lu min", nextSync);
              break;
            }
          default:
            {
              screenIndex = 0;
              refreshDisplay();
            }
        }
        break;
      }


    case 3:
      {
        DateTime rtc_now = rtc.now();
        DateTime ntp_now = DateTime(timeClient.getEpochTime());


        display.setCursor(0, 12);
        display.printf("RTC - %s\n", isRTCValid ? "Valid" : "Invalid");
        display.printf("%02d-%02d-%04d %02d:%02d:%02d\n\n", rtc_now.day(), rtc_now.month(), rtc_now.year(), rtc_now.hour(), rtc_now.minute(), rtc_now.second());
        display.printf("NTP - UTC+%d\n", conf.TIMEZONE_OFFSET);
        display.printf("%02d-%02d-%04d %02d:%02d:%02d\n", ntp_now.day(), ntp_now.month(), ntp_now.year(), ntp_now.hour(), ntp_now.minute(), ntp_now.second());


        display.setCursor(8, 64 - 8);
        display.print("Back");
        display.setCursor(64 + 8, 64 - 8);
        display.print("Sync");


        if (screenIndex > 1) screenIndex = 0;
        display.fillRect(0, 64 - 8, 4, 4, screenIndex == 0 ? WHITE : BLACK);
        display.fillRect(64, 64 - 8, 4, 4, screenIndex == 1 ? WHITE : BLACK);
        break;
      }


    default:
      {
        screenPage = 0;
        refreshDisplay();
        break;
      }
  }


  display.display();
}


// ==================== BUTTON FUNCTIONS ====================
ICACHE_RAM_ATTR void buttonPress() {
  button_time = millis();
  buttonState = true;
}


void onButtonShort() {
  Serial.println("Button Short");
  switch (screenPage) {
    case 0:
    case 1:
    case 2:
    case 3:
      {
        screenIndex++;
        refreshDisplay();
        break;
      }
  }
}


void onButtonLong() {
  Serial.println("Button Long");
  switch (screenPage) {
    case 0:
      {
        screenPage = 1;
        screenIndex = 0;
        refreshDisplay();
        break;
      }
    case 1:
      {
        switch (screenIndex) {
          case 0:
            {
              screenPage = 0;
              screenIndex = 0;
              refreshDisplay();
              break;
            }
          case 1:
            {
              screenPage = 2;
              screenIndex = 0;
              refreshDisplay();
              break;
            }
          case 2:
            {
              screenPage = 3;
              screenIndex = 0;
              refreshDisplay();
              break;
            }
          case 3:
            {
              conf.BOOT_TO_SETUP = true;
              saveConfig();
              ESP.restart();
              break;
            }
          case 4:
            {
              display.clearDisplay();
              display.setCursor(0, 0);
              display.println("Factory Reset?");
              display.println("This action will reset the sensor counter\n");
              display.println("Long press to continue");
              display.display();
              delay(2000);
              if (digitalRead(BUTTON_PIN) == LOW) {
                LittleFS.format();
                ESP.restart();
              }
              break;
            }
          case 5:
            {
              ESP.restart();
              break;
            }
        }
        break;
      }
    case 2:
      {
        switch (screenIndex) {
          case 0:
          case 1:
          case 2:
            {
              screenPage = 1;
              screenIndex = 0;
              refreshDisplay();
              break;
            }
          case 3:
            {
              // Manual sync RTC dari menu status
              display.clearDisplay();
              display.setCursor(0, 0);
              display.println("Syncing RTC...");
              display.display();

              if (syncRTC()) {
                display.println("Success!");
              } else {
                display.println("Failed!");
              }
              display.display();
              delay(2000);

              screenPage = 1;
              screenIndex = 0;
              refreshDisplay();
              break;
            }
        }
        break;
      }
    case 3:
      {
        if (screenIndex == 0) {
          screenPage = 1;
          screenIndex = 0;
          refreshDisplay();
        } else if (screenIndex == 1) {
          // Sync RTC
          display.clearDisplay();
          display.setCursor(0, 0);
          display.println("Syncing RTC...");
          display.display();

          if (syncRTC()) {
            display.println("\nSuccess!");
          } else {
            display.println("\nFailed!");
          }
          display.display();
          delay(2000);

          screenIndex = 0;
          refreshDisplay();
        }
        break;
      }
  }
}


// ==================== UTILITY FUNCTIONS ====================
byte internetCheck() {
  byte result = 0;
  if (WiFi.status() == WL_CONNECTED) {
    result = 1;
    if (Ping.ping(ping_ip, 1) > 0) {
      result = 2;
      if (Ping.maxTime() < 100) {
        result = 3;
      }
    }
  }
  return result;
}
