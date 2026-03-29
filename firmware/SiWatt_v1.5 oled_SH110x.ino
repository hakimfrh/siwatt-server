/*
  ==========================================
  SiWatt Smart Meter Firmware v1.4
  ==========================================
  Changelog dari v1.3:
  - Alur boot dan loop mengikuti SiWatt v1.2 (lebih stabil)
  - AUTO_SYNC_RTC default OFF (NTP sync hanya manual/via command)
  - Fix button navigation: screenIndex dibatasi per page
  - Fix onButtonLong: semua case punya default handler
  - Fix PREVENT_ZERO_READING: pakai counter, bukan langsung restart
  - Fix loadConfig: semua field punya default value
  - syncRTC() lebih simpel dan aman, tidak blocking lama
  - Tambah MQTT command: auto-sync-enable, auto-sync-disable
  - Menu dan aksi nya sekarang sesuai
  ==========================================
*/

#define FIRMWARE_VERSION "1.4"

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
#include <Adafruit_SH110X.h>
#include <ESP8266WebServer.h>

#define WHITE SH110X_WHITE
#define BLACK SH110X_BLACK

#if defined(ESP32)
#error "Software Serial is not supported on the ESP32"
#endif

#if !defined(PZEM_RX_PIN) && !defined(PZEM_TX_PIN)
#define PZEM_RX_PIN D5
#define PZEM_TX_PIN D6
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
  char USERNAME[32];
  char DEVICE_NAME[50];
  char DEVICE_LOCATION[50];
  char MQTT_OUT_RAW[64];
  char MQTT_IN_CMD[64];
  char MQTT_OUT_STATUS[64];
  uint16_t REPORT_INTERVAL;       // Interval kirim data (detik)
  bool AUTO_SYNC_RTC;             // Auto periodic sync RTC
  int8_t TIMEZONE_OFFSET;         // UTC offset (jam)
  uint16_t RTC_SYNC_INTERVAL;     // Interval auto sync RTC (jam)
  bool PREVENT_ZERO_READING;      // Restart jika PZEM baca 0
};

Conf conf = {
  .BOOT_TO_SETUP = false,
  .MQTT_PORT = 1883,
  .REPORT_INTERVAL = 1,
  .AUTO_SYNC_RTC = false,          // DEFAULT OFF - NTP sync manual saja
  .TIMEZONE_OFFSET = 7,
  .RTC_SYNC_INTERVAL = 24,
  .PREVENT_ZERO_READING = true
};

// ==================== SETUP MODE ====================
ESP8266WebServer server(80);
bool setupMode = false;

// ==================== HARDWARE ====================
IPAddress ping_ip(8, 8, 8, 8);

#define SCREEN_WIDTH 128
#define SCREEN_HEIGHT 64
Adafruit_SH1106G display(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, -1);
RTC_DS3231 rtc;

WiFiUDP ntpUDP;
NTPClient timeClient(ntpUDP, "id.pool.ntp.org", 25200, 60000);  // default UTC+7

WiFiClient espClient;
PubSubClient client(espClient);

// ==================== VARIABLES ====================
bool isRTCLostPower = false;
bool isBoot = true;
bool bootState = true;
bool buttonState = false;
unsigned long button_time = 0;
unsigned long lastUpdateMillis = 0;
unsigned long lastRTCSync = 0;
unsigned long time_2000 = 0;
unsigned long time_250 = 0;
unsigned long mqtt_rto = 0;
unsigned long lastPublish = 0;
unsigned long lastInternetCheck = 0;
byte internet = 0;
byte syncDelay = 0;
byte pzemErrorCount = 0;           // Counter untuk PZEM error berturut-turut

// Sensor data
float voltage = 0;
float current = 0;
float power = 0;
float energy = 0;
float frequency = 0;
float pf = 0;

// Display menu
// Menu: Back | Device Info | Sync RTC | Setup Mode | Factory Reset | Restart
const char* menu[] = { "Back", "Device Info", "Sync RTC", "Setup Mode", "Factory Reset", "Restart Device" };
const byte menuTotal = 6;
byte screenPage = 0;
byte screenIndex = 0;

/*
  ==================== PAGE STRUCTURE ====================
  Page 0: Sensor data (6 sub-views: all, V, A, W, Hz, pf)
    short press: cycle sub-view (0-5, wrap)
    long press: open menu (page 1)

  Page 1: Menu (6 items)
    short press: cycle menu items (0-5, wrap)
    long press: execute selected item
      0=Back(→page 0), 1=Device Info(→page 2), 2=Sync RTC(→page 3),
      3=Setup Mode, 4=Factory Reset, 5=Restart

  Page 2: Status Info (3 sub-pages: Device, WiFi, MQTT)
    short press: cycle sub-pages (0-2, wrap)
    long press: back to menu (page 1) dari manapun

  Page 3: RTC Sync (2 options: Back, Sync)
    short press: toggle Back/Sync (0-1, wrap)
    long press: execute (0=back to menu, 1=sync RTC)
  ==================== END PAGE STRUCTURE ====================
*/

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
bool hasInternet();
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
  if (!display.begin(0x3C, true)) {
    Serial.println(F("OLED gagal!"));
    while (1);
  }

  display.setTextColor(SH110X_WHITE);
  display.clearDisplay();
  display.setTextSize(2);
  display.setCursor(64 - (5 * 2 * 4), 16);
  display.print("Si Watt");
  display.setTextSize(1);
  display.setCursor(64 - (5 * 6), 48);
  display.printf("v%s", FIRMWARE_VERSION);
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
    while (1);
  } else {
    display.println(" OK");
  }

  isRTCLostPower = rtc.lostPower();
  if (isRTCLostPower) {
    display.println("WARN: RTC lost power!");
    display.display();
    Serial.println("WARNING: RTC lost power!");
  } else {
    display.println("RTC power OK");
    display.display();
    Serial.println("RTC power OK");
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
      while (1);
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
    return;
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

  // NTP setup - apply timezone offset
  timeClient.begin();
  timeClient.setTimeOffset(conf.TIMEZONE_OFFSET * 3600);
  Serial.printf("NTP timezone offset: UTC+%d\n", conf.TIMEZONE_OFFSET);

  // ---- BOOT SYNC RTC ----
  // Hanya sync saat boot jika RTC lost power DAN ada internet
  // Ini simple dan non-blocking (cuma 1x attempt)
  if (isRTCLostPower && WiFi.status() == WL_CONNECTED) {
    display.println("Sync RTC...");
    display.display();

    if (syncRTC()) {
      display.println("RTC Synced OK");
      isRTCLostPower = false;
      isBoot = false;
    } else {
      display.println("RTC Sync Failed");
      // Tetap lanjut, akan coba lagi di boot loop
    }
    display.display();
    delay(1000);
  } else if (!isRTCLostPower) {
    // RTC OK, skip boot sequence
    isBoot = false;
  }

  display.clearDisplay();
  display.drawLine(0, 9, 128, 9, WHITE);
  display.display();

  Serial.println("=== Setup Complete ===");
  Serial.printf("RTC Lost Power: %s\n", isRTCLostPower ? "Yes" : "No");
  Serial.printf("Boot Mode: %s\n", isBoot ? "Active" : "Skipped");
  Serial.printf("Auto Sync RTC: %s\n", conf.AUTO_SYNC_RTC ? "ON" : "OFF");
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
            // Long press in setup mode → cancel setup, restart normal
            conf.BOOT_TO_SETUP = false;
            saveConfig();
            ESP.restart();
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

  // Blink indicator saat boot atau MQTT disconnected
  if (now - time_250 >= 250) {
    time_250 = now;
    if (isBoot || !client.connected()) {
      if (bootState) display.fillRect(72, 4, 4, 4, WHITE);
      else display.fillRect(72, 4, 4, 4, BLACK);
      display.display();
      bootState = !bootState;
    } else if (!bootState) {
      display.fillRect(72, 4, 4, 4, BLACK);
      display.display();
    }
  }

  // ---- 1 SECOND TICK ----
  if (now - lastUpdateMillis >= 1000) {
    lastUpdateMillis = now;

    // Boot sync RTC - simple seperti v1.2
    if (isBoot) {
      if (internet == 3) {
        if (isRTCLostPower) {
          if (timeClient.isTimeSet()) {
            syncDelay++;
            if (syncDelay >= 5) {
              // NTP sudah stabil, sync RTC
              if (syncRTC()) {
                isRTCLostPower = false;
                isBoot = false;
                lastRTCSync = now;
                Serial.println("Boot RTC sync OK");
              } else {
                syncDelay = 3;  // Retry dari 3, bukan dari 0
                Serial.println("Boot RTC sync failed, retry...");
              }
            }
          }
        } else {
          isBoot = false;
          lastRTCSync = now;
        }
      }
    }

    // Publish sensor data sesuai interval
    if (!isBoot && !rtc.lostPower()) {
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

      // Periodic RTC sync (hanya jika enabled)
      checkPeriodicRTCSync();

    } else {
      // RTC error / masih boot
      if (screenPage == 0) {
        display.fillRect(0, 0, 6 * 10, 8, BLACK);
        display.setCursor(0, 0);
        display.print("Time Error");
        display.display();
      }
      readPzem();  // Tetap baca PZEM untuk update error count, tapi tidak publish
    }

    refreshDisplay();
  }

  // Internet check (ping) setiap 5 detik — tidak memblokir loop
  if (now - lastInternetCheck >= 5000) {
    lastInternetCheck = now;
    internet = internetCheck();
  }

  // Update WiFi signal setiap 2 detik
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
    server.send(400, "application/json", "{\"error\":\"Missing required fields: wifi_ssid, wifi_pass, username\"}");
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
  conf.AUTO_SYNC_RTC = doc["auto_sync_rtc"] | false;
  conf.PREVENT_ZERO_READING = doc["prevent_zero_reading"] | false;

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
  conf.AUTO_SYNC_RTC = doc["auto_sync_rtc"] | false;       // Default OFF
  conf.TIMEZONE_OFFSET = doc["timezone_offset"] | 7;
  conf.RTC_SYNC_INTERVAL = doc["rtc_sync_interval"] | 24;
  conf.PREVENT_ZERO_READING = doc["prevent_zero_reading"] | false;  // Default OFF

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
  Serial.printf("Auto Sync RTC: %s\n", conf.AUTO_SYNC_RTC ? "ON" : "OFF");
  Serial.printf("Prevent Zero: %s\n", conf.PREVENT_ZERO_READING ? "ON" : "OFF");

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
      // Manual sync via MQTT
      if (syncRTC()) {
        isRTCLostPower = false;
        if (isBoot) isBoot = false;
        client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"RTC synced successfully\"}");
      } else {
        client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"RTC sync failed\"}");
      }

    } else if (cmd == "factory-reset") {
      LittleFS.format();
      client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"Factory reset\"}");
      delay(500);
      ESP.restart();

    } else if (cmd == "set-interval") {
      uint16_t val = doc["value"] | 1;
      if (val < 1) val = 1;
      if (val > 3600) val = 3600;
      conf.REPORT_INTERVAL = val;
      saveConfig();

      JsonDocument response;
      response["status"] = "Interval updated";
      response["report_interval"] = conf.REPORT_INTERVAL;
      String responseStr;
      serializeJson(response, responseStr);
      client.publish(conf.MQTT_OUT_STATUS, responseStr.c_str());

    } else if (cmd == "set-timezone") {
      int8_t oldOffset = conf.TIMEZONE_OFFSET;
      conf.TIMEZONE_OFFSET = doc["value"] | 7;
      timeClient.setTimeOffset(conf.TIMEZONE_OFFSET * 3600);
      saveConfig();

      // Re-sync RTC karena timezone berubah
      // Jika tidak sync, RTC masih menyimpan waktu timezone lama
      bool synced = false;
      if (oldOffset != conf.TIMEZONE_OFFSET) {
        synced = syncRTC();
        if (synced) {
          isRTCLostPower = false;
          Serial.println("RTC re-synced after timezone change");
        } else {
          Serial.println("WARNING: Timezone changed but RTC re-sync failed!");
          Serial.println("RTC masih pakai waktu timezone lama.");
          Serial.println("Gunakan cmd sync-rtc untuk sync manual.");
        }
      }

      JsonDocument response;
      response["status"] = synced ? "Timezone updated & RTC synced" : "Timezone updated (RTC sync failed)";
      response["timezone"] = conf.TIMEZONE_OFFSET;
      response["rtc_synced"] = synced;
      String responseStr;
      serializeJson(response, responseStr);
      client.publish(conf.MQTT_OUT_STATUS, responseStr.c_str());

    } else if (cmd == "auto-sync-enable") {
      conf.AUTO_SYNC_RTC = true;
      lastRTCSync = millis();  // Reset timer
      saveConfig();
      client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"Auto sync RTC enabled\"}");

    } else if (cmd == "auto-sync-disable") {
      conf.AUTO_SYNC_RTC = false;
      saveConfig();
      client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"Auto sync RTC disabled\"}");

    } else if (cmd == "no-zero-read-enable") {
      conf.PREVENT_ZERO_READING = true;
      pzemErrorCount = 0;
      saveConfig();
      client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"Prevent zero reading enabled\"}");

    } else if (cmd == "no-zero-read-disable") {
      conf.PREVENT_ZERO_READING = false;
      pzemErrorCount = 0;
      saveConfig();
      client.publish(conf.MQTT_OUT_STATUS, "{\"status\":\"Prevent zero reading disabled\"}");

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
      info["auto_sync_rtc"] = conf.AUTO_SYNC_RTC;
      info["prevent_zero_reading"] = conf.PREVENT_ZERO_READING;
      info["rtc_lost_power"] = isRTCLostPower;

      // RTC time
      if (!rtc.lostPower()) {
        DateTime now = rtc.now();
        char buf[20];
        sprintf(buf, "%02d-%02d-%04d %02d:%02d:%02d", now.day(), now.month(), now.year(), now.hour(), now.minute(), now.second());
        info["rtc_time"] = buf;
      }

      String response;
      serializeJson(info, response);
      client.publish(conf.MQTT_OUT_STATUS, response.c_str());

    } else {
      // Unknown command
      JsonDocument response;
      response["error"] = "Unknown command";
      response["cmd"] = cmd;
      String responseStr;
      serializeJson(response, responseStr);
      client.publish(conf.MQTT_OUT_STATUS, responseStr.c_str());
    }
  }
}

void reconnect() {
  Serial.print("Attempting MQTT connection...");

  String clientId = "SWM-" + String(DEVICE_ID) + "-" + String(random(0xffff), HEX);

  bool connected = false;
  if (strlen(conf.MQTT_USER) > 0) {
    connected = client.connect(clientId.c_str(), conf.MQTT_USER, "");
  } else {
    connected = client.connect(clientId.c_str());
  }

  if (connected) {
    Serial.println("connected");
    client.subscribe(conf.MQTT_IN_CMD);

    // Publish online status (retained)
    JsonDocument doc;
    doc["device_id"] = DEVICE_ID;
    doc["status"] = "online";
    doc["uptime"] = millis() / 1000;
    doc["firmware"] = FIRMWARE_VERSION;

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

  // PREVENT_ZERO_READING: 3x percobaan, lalu stop dan restart
  if (conf.PREVENT_ZERO_READING && (newVoltage <= 0 || isnan(newVoltage))) {
    pzemErrorCount++;
    Serial.printf("PZEM Read Error. Attempt: %d/3\n", pzemErrorCount);

    if (pzemErrorCount >= 3) {
      Serial.println("PZEM SENSOR FAILED after 3 attempts");
      display.clearDisplay();
      display.setCursor(0, 0);
      display.setTextSize(1);
      display.println("PZEM SENSOR FAILED");
      display.println();
      display.println("3x read error");
      display.println("Check wiring/sensor");
      display.println();
      display.println("Restarting in 3s...");
      display.display();
      delay(3000);
      ESP.restart();
    }
    return false;
  }

  // Reset counter jika berhasil baca
  pzemErrorCount = 0;

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
/*
  syncRTC() - Sync RTC dengan NTP
  Simple dan non-blocking. Tidak ada retry loop.
  Return true jika berhasil, false jika gagal.
*/
bool syncRTC() {
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("No WiFi for RTC sync");
    return false;
  }

  if (!hasInternet()) {
    Serial.println("No internet for RTC sync");
    return false;
  }

  Serial.println("Syncing RTC with NTP...");

  // Force update NTP
  if (!timeClient.forceUpdate()) {
    Serial.println("NTP force update failed");
    return false;
  }

  if (!timeClient.isTimeSet()) {
    Serial.println("NTP time not set");
    return false;
  }

  unsigned long epochTime = timeClient.getEpochTime();

  // Validasi: epoch harus > 2020-01-01 (1577836800)
  if (epochTime < 1577836800) {
    Serial.printf("Invalid NTP epoch: %lu\n", epochTime);
    return false;
  }

  // Set RTC
  rtc.adjust(DateTime(epochTime));

  // Verify
  DateTime now = rtc.now();
  if (now.year() < 2020 || now.year() > 2100) {
    Serial.printf("RTC verify failed: year=%d\n", now.year());
    return false;
  }

  lastRTCSync = millis();
  Serial.printf("RTC synced: %02d-%02d-%04d %02d:%02d:%02d\n",
                now.day(), now.month(), now.year(),
                now.hour(), now.minute(), now.second());
  return true;
}

/*
  checkPeriodicRTCSync() - Auto sync RTC secara periodik
  Hanya aktif jika conf.AUTO_SYNC_RTC == true
*/
void checkPeriodicRTCSync() {
  if (!conf.AUTO_SYNC_RTC || conf.RTC_SYNC_INTERVAL == 0) {
    return;
  }

  unsigned long syncInterval = (unsigned long)conf.RTC_SYNC_INTERVAL * 3600000UL;
  unsigned long now = millis();

  // Handle millis overflow
  if (now < lastRTCSync) {
    lastRTCSync = now;
    return;
  }

  if (now - lastRTCSync >= syncInterval) {
    Serial.printf("Periodic RTC sync (every %d hours)\n", conf.RTC_SYNC_INTERVAL);

    if (syncRTC()) {
      Serial.println("Periodic sync OK");

      // Publish event ke MQTT
      if (client.connected()) {
        JsonDocument doc;
        doc["event"] = "rtc_synced";
        doc["type"] = "periodic";

        String msg;
        serializeJson(doc, msg);
        client.publish(conf.MQTT_OUT_STATUS, msg.c_str());
      }
    } else {
      Serial.println("Periodic sync failed");
      lastRTCSync = now;  // Reset timer agar tidak spam retry
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

  display.fillRect(120 - (8 + 6 * 5), 0, 8, 8, BLACK);
  for (int i = 0; i < bars; i++) {
    int h = (i + 1);
    display.fillRect(120 - (8 + 6 * 5) + i, 8 - h, 1, h, WHITE);
  }

  display.fillRect(120 - (6 * 5) + 3, 0, 6 * 5, 8, BLACK);
  display.setCursor(120 - (6 * 5) + 3, 0);

  if (internet == 0) display.print("X");
  else if (internet >= 2) display.printf("%02dms", Ping.maxTime());
  else display.print("- ms");

  display.display();
}

void refreshDisplay() {
  display.fillRect(0, 10, 128, 64 - 10, BLACK);

  switch (screenPage) {
    // ===== PAGE 0: Sensor Data =====
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
              return;
            }
        }
        break;
      }

    // ===== PAGE 1: Menu =====
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

    // ===== PAGE 2: Status Info (Device / WiFi / MQTT) =====
    case 2:
      {
        display.fillRect(0, 0, 6 * 10, 8, BLACK);
        display.setCursor(0, 0);
        display.setTextSize(1);
        display.print("Status");

        display.setCursor(0, 12);
        switch (screenIndex) {
          case 0:  // Device Info
            {
              unsigned long totalDetik = millis() / 1000;
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
          case 1:  // WiFi Info
            {
              display.printf("SSID: %s\n", WiFi.SSID().c_str());
              display.printf("WiFi: %s\n", WiFi.status() == WL_CONNECTED ? "Connected" : "Disconnected");
              display.printf("IP: ");
              display.println(WiFi.localIP());
              display.printf("GW: ");
              display.println(WiFi.gatewayIP());
              display.printf("Signal: %d dBm\n", WiFi.RSSI());
              break;
            }
          case 2:  // MQTT Info
            {
              display.printf("Serv: %s\n", conf.MQTT_SERVER);
              display.printf("MQTT: %s\n", client.connected() ? "Connected" : "Disconnected");
              display.printf("Port: %d\n", conf.MQTT_PORT);
              display.printf("User: %s\n", conf.MQTT_USER);
              break;
            }
          default:
            {
              screenIndex = 0;
              refreshDisplay();
              return;
            }
        }
        break;
      }

    // ===== PAGE 3: RTC Sync =====
    case 3:
      {
        DateTime rtc_now = rtc.now();
        DateTime ntp_now = DateTime(timeClient.getEpochTime());

        display.setCursor(0, 12);
        display.printf("RTC - Time %s\n", isRTCLostPower ? "(pwr lost)" : "");
        display.printf("%02d-%02d-%04d %02d:%02d:%02d\n\n", rtc_now.day(), rtc_now.month(), rtc_now.year(), rtc_now.hour(), rtc_now.minute(), rtc_now.second());
        display.printf("NTP - UTC+%d\n", conf.TIMEZONE_OFFSET);
        display.printf("%02d-%02d-%04d %02d:%02d:%02d\n", ntp_now.day(), ntp_now.month(), ntp_now.year(), ntp_now.hour(), ntp_now.minute(), ntp_now.second());

        // Bottom bar: Back / Sync
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
        screenIndex = 0;
        refreshDisplay();
        return;
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
    case 0:  // Sensor views: 0-5
      {
        screenIndex++;
        if (screenIndex > 5) screenIndex = 0;
        refreshDisplay();
        break;
      }
    case 1:  // Menu items: 0-(menuTotal-1)
      {
        screenIndex++;
        if (screenIndex >= menuTotal) screenIndex = 0;
        refreshDisplay();
        break;
      }
    case 2:  // Status sub-pages: 0-2
      {
        screenIndex++;
        if (screenIndex > 2) screenIndex = 0;
        refreshDisplay();
        break;
      }
    case 3:  // RTC sync: Back(0) / Sync(1)
      {
        screenIndex++;
        if (screenIndex > 1) screenIndex = 0;
        refreshDisplay();
        break;
      }
  }
}

void onButtonLong() {
  Serial.println("Button Long");
  switch (screenPage) {

    // ===== PAGE 0: Open Menu =====
    case 0:
      {
        screenPage = 1;
        screenIndex = 0;
        refreshDisplay();
        break;
      }

    // ===== PAGE 1: Execute Menu Item =====
    case 1:
      {
        switch (screenIndex) {
          case 0:  // Back → page 0
            {
              screenPage = 0;
              screenIndex = 0;
              refreshDisplay();
              break;
            }
          case 1:  // Device Info → page 2
            {
              screenPage = 2;
              screenIndex = 0;
              refreshDisplay();
              break;
            }
          case 2:  // Sync RTC → page 3
            {
              screenPage = 3;
              screenIndex = 0;
              refreshDisplay();
              break;
            }
          case 3:  // Setup Mode → restart ke setup
            {
              conf.BOOT_TO_SETUP = true;
              saveConfig();
              ESP.restart();
              break;
            }
          case 4:  // Factory Reset → confirm dulu
            {
              display.clearDisplay();
              display.setCursor(0, 0);
              display.println("Factory Reset?");
              display.println("This will erase all");
              display.println("config and data.\n");
              display.println("Hold button to confirm");
              display.println("Release to cancel");
              display.display();

              delay(2000);

              if (digitalRead(BUTTON_PIN) == LOW) {
                display.clearDisplay();
                display.setCursor(0, 0);
                display.println("Resetting...");
                display.display();
                LittleFS.format();
                delay(500);
                ESP.restart();
              } else {
                // Cancelled
                refreshDisplay();
              }
              break;
            }
          case 5:  // Restart Device
            {
              ESP.restart();
              break;
            }
          default:  // Safety: kembali ke page 0
            {
              screenPage = 0;
              screenIndex = 0;
              refreshDisplay();
              break;
            }
        }
        break;
      }

    // ===== PAGE 2: Status Info → always back to menu =====
    case 2:
      {
        screenPage = 1;
        screenIndex = 1;  // Kembali ke menu item "Device Info"
        refreshDisplay();
        break;
      }

    // ===== PAGE 3: RTC Sync → Back or Sync =====
    case 3:
      {
        if (screenIndex == 1) {
          // Sync RTC
          display.clearDisplay();
          display.setCursor(0, 0);
          display.println("Syncing RTC...");
          display.display();

          if (syncRTC()) {
            display.println("\nSync Success!");
            isRTCLostPower = false;
            if (isBoot) isBoot = false;
          } else {
            display.println("\nSync Failed!");
            display.println("Check internet");
          }
          display.display();
          delay(2000);

          screenIndex = 0;
          display.clearDisplay();
          refreshDisplay();
        } else {
          // Back → menu, cursor di "Sync RTC"
          screenPage = 1;
          screenIndex = 2;
          refreshDisplay();
        }
        break;
      }

    default:
      {
        screenPage = 0;
        screenIndex = 0;
        refreshDisplay();
        break;
      }
  }
}

// ==================== UTILITY FUNCTIONS ====================
/*
  internetCheck() - Ping ke 8.8.8.8, update cache 'internet' setiap 5 detik.
  Dipanggil dari timer di loop, BUKAN langsung saat butuh internet.
  Return:
    0 = WiFi tidak terhubung
    1 = WiFi terhubung, ping gagal
    2 = Ping OK
    3 = Ping OK, latency < 100ms
*/
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

/*
  hasInternet() - Ping langsung, dipakai sebelum operasi penting (sync RTC).
  Tidak mengupdate cache 'internet'.
*/
bool hasInternet() {
  if (WiFi.status() != WL_CONNECTED) return false;
  return Ping.ping(ping_ip, 1) > 0;
}
