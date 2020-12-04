package com.flink;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;


import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JsonUtil
 * @author ly
 * @date 2020/04/20
 */

public class JsonUtil {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    private static final JsonParser JSON_PARSER = new JsonParser();

    /**
     * @description json字符串转换成对象
     * @param json
     * @param clazz
     * @return
     */
    public static <T> T fromJson(String json, Class<T> clazz) throws Exception {
        T dto = null;
        try {
            dto = GSON.fromJson(json, clazz);
        } catch (JsonSyntaxException e) {
            throw new Exception("数据异常");
        }
        return dto;
    }

    /**
     * 以非严格模式将json字符串转换成对象
     * @param json
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> T fromJsonInLenient(String json, Class<T> clazz) throws Exception {
        T dto = null;
        try {
            JsonReader reader = new JsonReader(new StringReader(json));
            dto = GSON.fromJson(reader, clazz);
        } catch (JsonSyntaxException e) {
            throw new Exception("数据解析异常");
        }
        return dto;
    }

    /**
     * @description json array字符串转换成List对象
     * @param jsonArray
     * @param clazz
     * @return
     */
    public static <T> List<T> fromJsonArray(String jsonArray, Class<T> clazz) throws Exception {
        if (org.apache.commons.lang3.StringUtils.isBlank(jsonArray) || org.apache.commons.lang3.StringUtils.isBlank(jsonArray.trim())) {
            return null;
        }
        try {
            JsonArray array = strToJsonArray(jsonArray);
            if (array == null || array.size() == 0) {
                return null;
            }
            List<T> list = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                T dto = GSON.fromJson(array.get(i), clazz);
                list.add(dto);
            }
            return list;
        } catch (JsonSyntaxException e) {
            throw new Exception("数据异常");
        }
    }

    /**
     * @description json字符串转换成对象
     * @param json
     * @param clazz
     * @return
     */
    public static <T> T fromJson(JsonElement json, Class<T> clazz) throws Exception {
        T dto = null;
        try {
            dto = GSON.fromJson(json, clazz);
        } catch (JsonSyntaxException e) {
            throw new Exception("数据异常");
        }
        return dto;
    }

    /**
     * @description json字符串转换成对象
     * @param json
     * @param clazz
     * @return
     */
    public static <T> T fromJson(JsonElement json, Class<T> clazz, String format) throws Exception {
        T dto = null;
        try {
            Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat(format).create();
            dto = gson.fromJson(json, clazz);
        } catch (JsonSyntaxException e) {
            throw new Exception("数据异常");
        }
        return dto;
    }

    /**
     * @description json字符串转换成对象
     * @param json
     * @param clazz
     * @return
     */
    public static <T> T fromJson(String json, Class<T> clazz, String dateFormat) throws Exception {
        T dto = null;
        try {
            Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat(dateFormat).create();
            dto = gson.fromJson(json, clazz);
        } catch (JsonSyntaxException e) {
            throw new Exception("数据异常");
        }
        return dto;
    }

    public static JsonObject strToJson(String str) {
        if (org.apache.commons.lang3.StringUtils.isBlank(str)) {
            return null;
        }
        try {
            JsonObject json = JSON_PARSER.parse(str).getAsJsonObject();
            return json;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 以非严格模式将字符串转换成json对象
     * @param str
     * @return
     */
    public static JsonObject strToJsonInLenient(String str) {
        if (org.apache.commons.lang3.StringUtils.isBlank(str)) {
            return null;
        }
        try {
            JsonReader reader = new JsonReader(new StringReader(str));
            JsonObject json = JSON_PARSER.parse(reader).getAsJsonObject();
            return json;
        } catch (Exception e) {
            return null;
        }
    }

    public static JsonArray strToJsonArray(String str) {
        if (org.apache.commons.lang3.StringUtils.isBlank(str)) {
            return null;
        }
        try {
            JsonArray json = JSON_PARSER.parse(str).getAsJsonArray();
            return json;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * @description getStringFromJsonByKey
     * @param json
     * @param key
     * @return
     */
    public static String getString(JsonObject json, String key) {
        if (json == null) {
            return null;
        }
        if (key == null) {
            return null;
        }
        if (json.has(key)) {
            return json.get(key).getAsString();
        }
        return null;
    }

    /**
     * @description getDouble
     * @param json
     * @param key
     * @return
     */
    public static Double getDouble(JsonObject json, String key) {
        if (json == null || org.apache.commons.lang3.StringUtils.isBlank(key)) {
            return null;
        }
        if (json.has(key)) {
            return json.get(key).getAsDouble();
        }
        return null;
    }

    /**
     * @description getInteger
     * @param json
     * @param key
     * @return
     */
    public static Integer getInteger(JsonObject json, String key) {
        if (json == null || org.apache.commons.lang3.StringUtils.isBlank(key)) {
            return null;
        }
        if (json.has(key)) {
            return json.get(key).getAsInt();
        }
        return null;
    }

    /**
     * @description getInteger
     * @param json
     * @param key
     * @return
     */
    public static Long getLong(JsonObject json, String key) {
        if (json == null || org.apache.commons.lang3.StringUtils.isBlank(key)) {
            return null;
        }
        if (json.has(key)) {
            return json.get(key).getAsLong();
        }
        return null;
    }

    /**
     * @description getJsonArrayFronJsonByKey
     * @param json
     * @param key
     * @return
     */
    public static JsonObject getJsonObject(JsonObject json, String key) {
        if (json == null) {
            return null;
        }
        if (json.has(key)) {
            try {
                return json.get(key).getAsJsonObject();
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    /**
     * @description getJsonArrayFronJsonByKey
     * @param json
     * @param key
     * @return
     */
    public static JsonArray getJsonArray(JsonObject json, String key) {
        if (json == null) {
            return null;
        }
        if (json.has(key)) {
            try {
                return json.get(key).getAsJsonArray();
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    /**
     * @description 对象转换成json字符串
     * @param obj
     * @return
     */
    public static String toJson(Object obj){
        if (obj == null) {
            return null;
        }
        String result = null;
        try {
            result = GSON.toJson(obj);
        } catch (Exception e) {
            return null;
        }
        return result;
    }
    /**
     * @description 对象转换成json字符串
     * @param obj
     * @return
     */
    public static String toJson(Object obj, String dateFormat){
        Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat(dateFormat).create();
        if (obj == null) {
            return null;
        }
        String result = null;
        try {
            result = gson.toJson(obj);
        } catch (Exception e) {
            return null;
        }
        return result;
    }

    /**
     * @description 对象转换成json字符串
     * @param json
     * @return
     */
    public static JsonArray toJsonArray(String json) throws Exception {
        JsonArray ja = null;
        try {
            ja = JSON_PARSER.parse(json).getAsJsonArray();
        } catch (Exception e) {
            throw new Exception("json格式不对");
        }
        return ja;
    }

    /**
     * 对象转json对象
     * @param obj
     * @return
     */
    public static JsonObject objToJson(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            String json = toJson(obj);
            return JSON_PARSER.parse(json).getAsJsonObject();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 对象转JsonArray对象
     * @param obj
     * @return
     */
    public static JsonArray objToJsonArray(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            String json = toJson(obj);
            return strToJsonArray(json);
        } catch (Exception e) {
            return null;
        }
    }

    public static void main(String[] args) {
        List<Object> list = new ArrayList<>();
        System.out.println(toJson(list));
        Map<String, Object> map = new HashMap<>();
        map.put("fff", "fff");
        System.out.println(JsonUtil.objToJson(map).toString());
        JsonObject json = JsonUtil.objToJson(map);
        json.addProperty("fff1", "1234");
        json.addProperty("fff2", "1234");
        json.remove("fff");
        System.out.println(json.toString());
    }
}
