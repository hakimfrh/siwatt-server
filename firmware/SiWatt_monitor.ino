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


const char* DEVICE_ID = []() -> const char* {
  static char idStr[7];  // 6 karakter + null terminator
  uint64_t chipId;

#ifdef ESP8266
  chipId = ESP.getChipId();  // 32-bit
#elif defined(ESP32)
  chipId = ESP.getEfuseMac();  // 64-bit
#endif

  const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  const int len = sizeof(alphanum) - 1;

  for (int i = 0; i < 6; i++) {
    idStr[i] = alphanum[(chipId >> (i * 5)) % len];
  }
  idStr[6] = '\0';  // null terminator wajib

  return idStr;
}();


// config variables
#define CONFIG_PATH "/conf.txt"
struct Conf {
  char WLAN_SSID[32];
  char WLAN_PASS[32];
  String MQTT_SERVER;
  uint16_t MQTT_PORT;
  char MQTT_OUT_RAW[25];
  char MQTT_IN_CMD[25];
  char MQTT_OUT_CMD[25];
};

Conf conf;

IPAddress ping_ip(8, 8, 8, 8);

#define SCREEN_WIDTH 128
#define SCREEN_HEIGHT 64
Adafruit_SSD1306 display(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, -1);  // -1 = no reset pin
RTC_DS3231 rtc;

// NTP config
WiFiUDP ntpUDP;
NTPClient timeClient(ntpUDP, "id.pool.ntp.org", 25200, 60000);  // 25200 = GMT+7, update tiap 60 detik

WiFiClient espClient;
PubSubClient client(espClient);

bool isBoot = true;
bool bootState = true;
bool buttonState = false;
unsigned long button_time = 0;
unsigned long lastUpdateMillis = 0;
unsigned long time_2000 = 0;
unsigned long time_250 = 0;
unsigned long mqtt_rto = 0;
byte internet = 0;
byte syncDelay = 0;
float voltage = 0;
float current = 0;
float power = 0;
float energy = 0;
float frequency = 0;
float pf = 0;

const char* menu[] = { "Back", "Sync RTC", "WiFi Setting", "MQTT Setting", "Setup Mode", "Restart Device" };
byte screenPage = 0;
byte screenIndex = 0;

void setup() {
  Serial.begin(115200);
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
  delay(500);

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
  } else display.println(" OK");
  display.print("Loading Config");
  display.display();
  Serial.println("Loading conf...");
  if (!loadConfig()) {
    Serial.println("config error, use default");
    display.println(" FAIL");
    display.println("load default...");
    display.display();

    strncpy(conf.WLAN_SSID, "ZIHAN", sizeof(conf.WLAN_SSID));
    strncpy(conf.WLAN_PASS, "753695123", sizeof(conf.WLAN_PASS));
    conf.MQTT_SERVER = "broker.emqx.io";
    conf.MQTT_PORT = 1883;
    snprintf(conf.MQTT_OUT_RAW, sizeof(conf.MQTT_OUT_RAW), "/hakimfrh/swm-raw/%s", DEVICE_ID);
    snprintf(conf.MQTT_IN_CMD, sizeof(conf.MQTT_IN_CMD), "/hakimfrh/swm-cmd/%s", DEVICE_ID);
    snprintf(conf.MQTT_OUT_CMD, sizeof(conf.MQTT_OUT_CMD), "/hakimfrh/swm-rsp/%s", DEVICE_ID);
    delay(100);
    // WiFi.beginSmartConfig();
    // Serial.println("Waiting for Smartconf...");
    // while (!WiFi.smartConfigDone()) {
    //   delay(500);
    //   Serial.print(".");
    // }
    // Serial.println("\nSmartConfig received.");
  } else display.println(" OK");

  display.println("Setup WiFi...");
  display.display();
  Serial.println("Connecting WiFi...");
  WiFi.hostname(String("SiWatt Meter - ") + DEVICE_ID);
  WiFi.begin(conf.WLAN_SSID, conf.WLAN_PASS);
  // if (internetCheck() == 2) {
  //   syncRTC();
  // }

  randomSeed(micros());
  display.println("Setup MQTT...");
  display.display();
  Serial.println("Set MQTT...");
  client.setServer(conf.MQTT_SERVER.c_str(), conf.MQTT_PORT);
  client.setCallback(callback);

  timeClient.begin();

  display.clearDisplay();
  display.drawLine(0, 9, 128, 9, WHITE);
}

void loop() {
  unsigned long now = millis();
  // unsigned long rtt = millis();
  if (buttonState) {
    if (digitalRead(BUTTON_PIN) == HIGH) {
      buttonState = false;
      if (now - button_time >= 2000) {  //long press
        onButtonLong();
      } else {  //short press
        onButtonShort();
      }
    } else {
      if (now - button_time >= 2000) {
        buttonState = false;
        onButtonLong();
      }
    }
  }
  // Serial.printf("button: %d ", millis() - now);
  // rtt = millis();
  if (!client.connected()) {
    if (now - mqtt_rto >= 5000) {
      mqtt_rto = now;
      if (internet == 3) reconnect();
    }
  }
  // Serial.printf("mqtt: %d ", millis() - rtt);
  // rtt = millis();
  if (now - time_250 >= 250) {
    time_250 = now;
    if (isBoot | !client.connected()) {
      if (bootState) display.fillRect(80, 4, 4, 4, WHITE);
      else display.fillRect(80, 4, 4, 4, BLACK);
      display.display();
      bootState = !bootState;
    } else if (!bootState) {
      display.fillRect(80, 4, 4, 4, BLACK);
      display.display();
    }
  }
  // Serial.printf("blink: %d ", millis() - rtt);
  // rtt = millis();

  // 1. Update waktu tiap 1 detik
  if (now - lastUpdateMillis >= 1000) {
    lastUpdateMillis = now;
    internet = internetCheck();
    // Serial.printf("ping: %d ", millis() - rtt);
    // rtt = millis();
    if (isBoot) {
      if (internet == 3) {
        // Serial.printf("inter: %d  rtc %s  ntp: %s\n", internet, rtc.lostPower()?"true":"false" ,timeClient.isTimeSet() ? "true" : "false");
        if (rtc.lostPower()) {
          if (timeClient.isTimeSet()) {
            syncDelay++;
            if (syncDelay >= 5) {
              rtc.adjust(DateTime(timeClient.getEpochTime()));
              isBoot = false;
            }
          }
        } else {
          isBoot = false;
        }
      }
    } else display.fillRect(4, 64, 4, 4, BLACK);
    // Serial.printf("boot: %d ", millis() - rtt);
    // rtt = millis();

    if (!rtc.lostPower()) {
      DateTime rtc_now = rtc.now();  // ambil waktu sekarang
      char waktu[9];
      sprintf(waktu, "%02d:%02d:%02d", rtc_now.hour(), rtc_now.minute(), rtc_now.second());

      char tanggal[11];
      sprintf(tanggal, "%02d-%02d-%04d", rtc_now.day(), rtc_now.month(), rtc_now.year());

      if (readPzem()) {
        // Serial.printf("read-pzem: %d ", millis() - rtt);
        // rtt = millis();
        // simpan ke stack buat dikirim.
        JsonDocument doc;
        doc["datetime"] = String(tanggal) + " " + String(waktu);
        doc["voltage"] = voltage;
        doc["current"] = current;
        doc["power"] = power;
        doc["energy"] = energy;
        doc["frequency"] = frequency;
        doc["pf"] = pf;
        doc["ping"] = Ping.maxTime();
        doc["rssi"] = WiFi.RSSI();

        doc["device_id"] = DEVICE_ID;
          String data;
          serializeJson(doc, data);
        // Serial.printf("json-pzem: %d ", millis() - rtt);
        // rtt = millis();
        // Serial.printf("send data to %s\ndata: %s\n", conf.MQTT_OUT_RAW, data.c_str());
        if (client.connected()) client.publish(conf.MQTT_OUT_RAW, data.c_str());
      }
      // Serial.printf("end-pzem: %d ", millis() - rtt);
      // rtt = millis();
      // Serial.print(rtc_now.hour());
      // Serial.print(rtc_now.minute());
      // Serial.println(rtc_now.second());
      // Serial.println(waktu);
      // Serial.println(tanggal);
      if (screenPage == 0) {
        display.fillRect(0, 0, 6 * 10, 8, BLACK);
        display.setCursor(0, 0);
        display.print(waktu);
        display.display();
      }
    } else {
      if (screenPage == 0) {
        display.fillRect(0, 0, 6 * 10, 8, BLACK);
        display.setCursor(0, 0);
        display.print("Time Error");
        display.display();
      }
    }
    refreshDisplay();
  }
  // Serial.printf("end-1s: %d ", millis() - rtt);
  // rtt = millis();

  if (now - time_2000 >= 2000) {
    time_2000 = now;
    updateWiFiSignal();  // Fungsi buat update sinyal + OLED
  }
  // Serial.printf("end-2s: %d ", millis() - rtt);
  // rtt = millis();

  client.loop();
  // Serial.printf("client-loop: %d ", millis() - rtt);
  // Serial.printf("total: %d ", millis() - now);
  // if (internet == 3)
  timeClient.update();
  //   Serial.printf("time-update: %d ", millis() - rtt);
  // rtt = millis();
  // Serial.println();
}

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
    case 0:  //openMenu
      {
        screenPage++;
        screenIndex = 0;
        refreshDisplay();
        break;
      }
    case 1:
      {
        // const char* menu[] = { "Back", "Sync RTC", "WiFi Setting", "MQTT Setting", "Setup Mode", "Restart Device" };
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
              screenPage = 2;  //sync rtc
              screenIndex = 0;
              refreshDisplay();
              break;
            }
          case 3:
            {
              screenPage = 4;
              screenIndex = 0;
              refreshDisplay();
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
    case 2:  //sync rtc
      {
        switch (screenIndex) {
          case 0:
            {
              screenPage = 1;
              screenIndex = 0;
              refreshDisplay();
              break;
            }
          case 1:
            {
              syncRTC();
              screenIndex = 0;
              refreshDisplay();
              break;
            }
        }
        break;
      }
    case 4:  //mqtt
      {
        screenPage = 1;
        screenIndex = 0;
        refreshDisplay();
        break;
      }
  }
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
              display.printf("P:%0.1f W  pf:%0.2f ", frequency, pf);
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
              display.printf("F:%0.1f Hz  pf:%0.2f ", frequency, pf);
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
        byte menuTotal = 6;
        if (screenIndex >= menuTotal) screenIndex = 0;
        display.fillRect(0, 0, 6 * 10, 8, BLACK);
        display.setCursor(0, 0);
        display.setTextSize(1);
        display.print("Menu");
        // display.fillRect(0, 10, 128, 64 - 10, BLACK);
        display.fillRect(0, 12, 8, 64 - 12, BLACK);
        display.fillRect(0, map(screenIndex, 0, 5, 12, 13 + (7 * 6)), 4, 4, WHITE);
        display.setCursor(8, 12);
        for (byte i = 0; i < menuTotal; i++) {
          display.setCursor(8, 12 + 8 * i);
          display.printf("%s", menu[i]);
        }

        break;
      }
    case 2:  //sync rtc
      {
        DateTime rtc_now = rtc.now();                            // ambil waktu sekarang
        DateTime ntp_now = DateTime(timeClient.getEpochTime());  // ambil waktu sekarang

        display.setCursor(0, 12);
        display.printf("RTC - Time\n");
        display.printf("%02d-%02d-%04d %02d:%02d:%02d\n\n", rtc_now.day(), rtc_now.month(), rtc_now.year(), rtc_now.hour(), rtc_now.minute(), rtc_now.second());
        display.printf("NTP - Time\n");
        display.printf("%02d-%02d-%04d %02d:%02d:%02d\n\n", ntp_now.day(), ntp_now.month(), ntp_now.year(), ntp_now.hour(), ntp_now.minute(), ntp_now.second());

        display.setCursor(8, 64 - 8);
        display.print("Back");
        display.setCursor(64 + 8, 64 - 8);
        display.print("Sync");

        if (screenIndex > 1) screenIndex = 0;
        display.fillRect(0, 64 - 8, 4, 4, screenIndex == 0 ? WHITE : BLACK);
        display.fillRect(64, 64 - 8, 4, 4, screenIndex == 1 ? WHITE : BLACK);
        break;
      }
    case 3:  //wifi setting
    case 4:  //mqtt setting
      {
        display.setCursor(0, 12);
        display.printf("MQTT Status: %s\n", client.connected() ? "UP" : "DOWN");
        display.printf("Server: %s\n", conf.MQTT_SERVER.c_str());
        display.printf("Port: %d\n", conf.MQTT_PORT);

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

// ====================== MQTT FUNCTION ======================
void callback(char* topic, byte* payload, unsigned int length) {
  String data;
  for (unsigned int i = 0; i < length; i++) {
    data += (char)payload[i];
  }
  Serial.printf("MSG (%s): %s\n", topic, data);
  if (strcmp(topic, conf.MQTT_IN_CMD) == 0) {
    if (data == "pzem-reset") {
      pzem.resetEnergy();
      Serial.println("PZEM ENERGY RESET");
      client.publish(conf.MQTT_OUT_CMD, "PZEM ENERGY RESET");
    }
    if (data == "reboot") {
      Serial.println("REBOOT...");
      client.publish(conf.MQTT_OUT_CMD, "REBOOT...");
      delay(500);
      ESP.reset();
    }
    if (data == "sync-rtc") {
      syncRTC();
      client.publish(conf.MQTT_OUT_CMD, "RTC SYNC");
    }
    if (data == "get-config") {
    }
  }
}

void reconnect() {
  // Loop until we're reconnected

  Serial.print("Attempting MQTT connection...");
  // Create a random client ID
  String clientId = "SWM-" + String(DEVICE_ID);
  clientId += String(random(0xffff), HEX);
  Serial.println(clientId);
  // Attempt to connect
  if (client.connect(clientId.c_str())) {
    client.subscribe(conf.MQTT_IN_CMD);
  } else {
    Serial.print("failed, rc=");
    Serial.println(client.state());
    // Serial.println(" try again in 5 seconds");
    // Wait 5 seconds before retrying
    // delay(5000);
  }
}



// ====================== PZEM SENSOR ======================
bool readPzem() {
  float newVoltage = pzem.voltage();
  float newCurrent = pzem.current();
  float newPower = pzem.power();
  float newEnergy = pzem.energy();
  float newFrequency = pzem.frequency();
  float newPf = pzem.pf();
  bool error = false;

  // Check if the data is valid
  if (isnan(newVoltage)) {
    Serial.println("Error reading voltage");
    newVoltage = 0;
    error = true;
  }
  if (isnan(newCurrent)) {
    Serial.println("Error reading current");
    newCurrent = 0;
    error = true;
  }
  if (isnan(newPower)) {
    Serial.println("Error reading power");
    newPower = 0;
    error = true;
  }
  if (isnan(newEnergy)) {
    Serial.println("Error reading energy");
    newEnergy = 0;
    error = true;
  }
  if (isnan(newFrequency)) {
    Serial.println("Error reading frequency");
    newFrequency = 0;
    error = true;
  }
  if (isnan(newPf)) {
    Serial.println("Error reading power factor");
    newPf = 0;
    error = true;
  }

  voltage = newVoltage;
  current = newCurrent;
  frequency = newFrequency;
  power = newPower;
  energy = newEnergy;
  pf = newPf;

  // Serial.print("Voltage: ");
  // Serial.print(voltage);
  // Serial.println("V");
  // Serial.print("Current: ");
  // Serial.print(current);
  // Serial.println("A");
  // Serial.print("Power: ");
  // Serial.print(power);
  // Serial.println("W");
  // Serial.print("Energy: ");
  // Serial.print(energy, 3);
  // Serial.println("kWh");
  // Serial.print("Frequency: ");
  // Serial.print(frequency, 1);
  // Serial.println("Hz");
  // Serial.print("PF: ");
  // Serial.println(pf);

  return !error;
}

// ====================== SIGNAL BAR ======================
void updateWiFiSignal() {
  int rssi = WiFi.RSSI();  // Ambil nilai sinyal
  int bars = 0;

  if (rssi < -90) {
    rssi = -90;
  }
  if (rssi > -60) {
    rssi = -60;
  }
  bars = map(rssi, -90, -60, 0, 7);

  display.fillRect(128 - (8 + 6 * 5), 0, 8, 8, BLACK);  // clear area pojok kanan atas
  for (int i = 0; i < bars; i++) {
    int h = (i + 1);
    display.fillRect(128 - (8 + 6 * 5) + i, 8 - h, 1, h, WHITE);  // bar kecil-kecil
  }

  display.fillRect(128 - (6 * 5) + 3, 0, 6 * 5, 8, BLACK);  // clear area pojok kanan atas
  display.setCursor(128 - (6 * 5) + 3, 0);
  // byte status = internetCheck();
  if (internet == 0) display.print("X");
  else if (internet >= 2) display.printf("%02dms", Ping.maxTime());
  else display.print("- ms");
  display.display();  // tampilkan
}

void syncRTC() {
  if (internetCheck() == 3) {
    rtc.adjust(DateTime(timeClient.getEpochTime()));
    Serial.println("RTC synced!");
  }
}

/*
====================== Internet Check ======================.
return byte.
0 = no connection.
1 = wifi connected, no internet.
2 = internet available.
3 = ping ok.
*/
byte internetCheck() {
  byte result = 0;
  if (WiFi.status() == WL_CONNECTED) {
    result = 1;
    if (Ping.ping(ping_ip, 1) > 0) {
      result = 2;
      if (Ping.maxTime() < 100) {
        // Serial.println(Ping.maxTime());
        result = 3;
      }
    }
  }
  return result;
}

// ====================== CONFIG ======================
bool saveConfig() {
  JsonDocument doc;
  doc["ssid"] = conf.WLAN_SSID;
  doc["pass"] = conf.WLAN_PASS;

  String conf;
  serializeJson(doc, conf);
  if (fsWrite(CONFIG_PATH, conf)) return true;
  else return false;
}

bool loadConfig() {
  String file = fsRead(CONFIG_PATH);
  if (file.length() > 0) {
    JsonDocument doc;
    DeserializationError error = deserializeJson(doc, file);
    if (error) {
      Serial.println("Failed to load config: deserialize fail");
      return false;
    } else {
      strncpy(conf.WLAN_SSID, doc["ssid"] | "", sizeof(conf.WLAN_SSID));
      strncpy(conf.WLAN_PASS, doc["pass"] | "", sizeof(conf.WLAN_PASS));
      return true;
    }
  } else {
    Serial.println("Failed to load config: no config");
    return false;
  }
}

// ====================== LITTLE FS ======================
String fsRead(String path) {
  File file = LittleFS.open(path, "r");
  if (!file) {
    Serial.println("Failed to open file for reading");
    return "";
  }

  // Read from the file
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
  file.print(data);
  file.close();
  return true;
}