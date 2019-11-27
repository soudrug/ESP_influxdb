#include "Arduino.h"
#include "ESPinfluxdb.h"


#define DEBUG_PRINT // comment this line to disable debug print

#ifndef DEBUG_PRINT
#define DEBUG_PRINT(a)
#else
//#define DEBUG_PRINT(a) (Serial.println(String(("[EIDB]: "))+(a)))
#define DEBUG_PRINT(a) (Serial.println(a))
#define _DEBUG
#endif

#ifdef ESP32
#include <WiFiClientSecure.h>
#endif

Influxdb::Influxdb(String host, uint16_t port) {
        _url = "http://" + host + ":" + port;
}

Influxdb::Influxdb(const char *url) {
        _url = url;
}

Influxdb::Influxdb(const char *url, const char *certThumbPrint):Influxdb(url) {
        _certThumbPrint = certThumbPrint;
}

DB_RESPONSE Influxdb::opendb(String db, String user, String password) {
        HTTPClient http;
         _response = DB_ERROR;
         _lastErrorResponse = "";
#ifdef ESP32          
        WiFiClientSecure *client = new WiFiClientSecure;
        client -> setCACert(_certThumbPrint.c_str());
#endif         
        String queryUrl = _url+ "/query?q=show%20databases" + "&u=" + user + "&p=" + password; 
        if(_url.startsWith("https")) {
          DEBUG_PRINT("https request: " + queryUrl);
#ifdef ESP32          
          if(!http.begin(*client,queryUrl)) { //HTTPs
#else        
          if(!http.begin(queryUrl, _certThumbPrint)) { //HTTPs        
#endif          
            DEBUG_PRINT("begin failed");
            _response = DB_ERROR;
            return _response;
          }
        } else {
          DEBUG_PRINT("http request: " + queryUrl);
          http.begin(queryUrl); //HTTP
        }
        DEBUG_PRINT("Issueing get");
        int httpCode = http.GET();
        DEBUG_PRINT(String("Status: ") + String(httpCode));
        if (httpCode == 200) {
                String payload = http.getString();
                if (payload.indexOf("[\""+db+"\"]") > 0) {
                   _db = db + "&u=" + user + "&p=" + password;
                   _response = DB_SUCCESS;
                } else {
                  DEBUG_PRINT("Database not found");
                }
         } else {
          DEBUG_PRINT("Database open failed");
          _response = DB_ERROR;
          if(httpCode > 0 ) {
            DEBUG_PRINT(String("Payload: ") + http.getString());
            _lastErrorResponse = http.getString();
          }
        }
#ifdef ESP32
        delete client;
#endif        
        http.end(); 
        return _response;
}

DB_RESPONSE Influxdb::opendb(String db) {
         _lastErrorResponse = "";
        HTTPClient http;
        String queryUrl = _url+ "/query?q=show%20databases"; 
        DEBUG_PRINT("requestUrl: " + queryUrl);
        if(_url.startsWith("https")) {
#ifdef ESP32          
            http.begin(queryUrl, _certThumbPrint.c_str()); //HTTPs
#else        
            http.begin(queryUrl, _certThumbPrint); //HTTPs
#endif         
        } else {
          http.begin(queryUrl); //HTTP
        }

        int httpCode = http.GET();

        if (httpCode == 200) {
                _response = DB_SUCCESS;
                String payload = http.getString();
               if (payload.indexOf(db) > 0) {
                        _db =  db;
                } else {
                  DEBUG_PRINT("Database not found");
                }
        } else {
          DEBUG_PRINT("Status: " + httpCode);
          DEBUG_PRINT("Payload: " + http.getString());
          _response = DB_ERROR;
          _lastErrorResponse = http.getString();
          DEBUG_PRINT("Database open failed");
        }
         http.end();
        
        return _response;

}

DB_RESPONSE Influxdb::write(dbMeasurement data) {
        return write(data.postString());
}

DB_RESPONSE Influxdb::write(String data, String precision) {
         _lastErrorResponse = "";
        HTTPClient http;

        DEBUG_PRINT("HTTP post begin...");
        String queryUrl = _url+ "/write?db=" + _db;
        if(precision != "") {
          queryUrl += "&precision=" +precision;
        }
        DEBUG_PRINT("requestUrl: " + queryUrl);
        if(_url.startsWith("https")) {
#ifdef ESP32          
            http.begin(queryUrl, _certThumbPrint.c_str()); //HTTPs
#else        
            http.begin(queryUrl, _certThumbPrint); //HTTPs
#endif         
        } else {
          http.begin(queryUrl); //HTTP
        }
        http.addHeader("Content-Type", "text/plain");

        int httpResponseCode = http.POST(data);

        if (httpResponseCode == 204) {
                _response = DB_SUCCESS;
                String response = http.getString();    //Get the response to the request
                DEBUG_PRINT(String(httpResponseCode)); //Print return code
                DEBUG_PRINT(response);                 //Print request answer

        } else {
                _lastErrorResponse = http.getString();
                DEBUG_PRINT("Error on sending POST:");
                DEBUG_PRINT(String(httpResponseCode));
                _response=DB_ERROR;
        }

        http.end();
        return _response;
}

String Influxdb::query(String sql) {
         _lastErrorResponse = "";
        String ret = "";
        String url = "/query?";
        url += "pretty=true&";
        url += "db=" + _db;
        url += "&q=" + URLEncode(sql);
        DEBUG_PRINT("Requesting URL: ");
        DEBUG_PRINT(url);

        HTTPClient http;

        String queryUrl = _url+ url;
        if(_url.startsWith("https")) {
#ifdef ESP32          
            http.begin(queryUrl, _certThumbPrint.c_str()); //HTTPs
#else        
            http.begin(queryUrl, _certThumbPrint); //HTTPs
#endif         
        } else {
          http.begin(queryUrl); //HTTP
        }
  
        // start connection and send HTTP header
        int httpCode = http.GET();

        // httpCode will be negative on error
        if (httpCode == 200) {
                // HTTP header has been send and Server response header has been handled
                _response = DB_SUCCESS;
                ret = http.getString();
                Serial.println(ret);

        } else {
                _response = DB_ERROR;
                 _lastErrorResponse = http.getString();
                DEBUG_PRINT("[HTTP] GET... failed, error: " + httpCode);
        }

        http.end();
        return ret;
}


DB_RESPONSE Influxdb::response() {
        return _response;
}

/* -----------------------------------------------*/
//        Field object
/* -----------------------------------------------*/
dbMeasurement::dbMeasurement(String m) {
        measurement = m;
        empty();
}

void dbMeasurement::empty() {
        _data = "";
        _tag = "";
        _tagCount = 0;
        _fieldCount = 0;
}

void dbMeasurement::clearFields() {
        _data = "";
        _fieldCount = 0;
}

void dbMeasurement::addTag(String key, String value) {
        _tag += "," + key + "=" + value;
        ++_tagCount;
}

void dbMeasurement::addField(String key, float value) {
        _data = (_data == "") ? (" ") : (_data += ",");
        _data += key + "=" + String(value);
        ++_fieldCount;
}
void dbMeasurement::addField(String key, String value) {
       _data = (_data == "") ? (" ") : (_data += ",");
        _data += key + "=\"" + value + "\"";
        ++_fieldCount;
}

String dbMeasurement::postString() {
        //  uint32_t utc = 1448114561 + millis() /1000;
        return measurement + _tag + _data;
}

int dbMeasurement::fieldCount() {
    return _fieldCount;
}

int dbMeasurement::tagCount() {
    return _tagCount;
}

// URL Encode with Arduino String object
String URLEncode(String msg) {
        const char *hex = "0123456789abcdef";
        String encodedMsg = "";

        uint16_t i;
        for (i = 0; i < msg.length(); i++) {
                if (('a' <= msg.charAt(i) && msg.charAt(i) <= 'z') ||
                    ('A' <= msg.charAt(i) && msg.charAt(i) <= 'Z') ||
                    ('0' <= msg.charAt(i) && msg.charAt(i) <= '9')) {
                        encodedMsg += msg.charAt(i);
                } else {
                        encodedMsg += '%';
                        encodedMsg += hex[msg.charAt(i) >> 4];
                        encodedMsg += hex[msg.charAt(i) & 15];
                }
        }
        return encodedMsg;
}
