import 'dart:async';
import 'dart:convert';

import 'package:rethinkdb_driver2/rethinkdb_driver2.dart';
import 'package:http_client/console.dart' as http;

class localityConverter {
  Rethinkdb _r;
  Connection _rethinkConn;
  final Map _config;
  /// Таблица tasks
  String get _tasksTableName => _config["tasksTableName"];
  /// http клиент для выполнения запросов к dadata
  http.Client _client = new http.ConsoleClient();

  localityConverter(this._r, this._rethinkConn, this._config);

  /// Основной url для получения подсказок.
  final String _baseSuggestUrl = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/";
  /// url для определения геопозиционирования.
  final String _detectAddressByIpUrl = 'https://suggestions.dadata.ru/suggestions/api/4_1/rs/detectAddressByIp?';
  /// КЛАДР. Используется для сортировки подсказок в соответствии с геолокацией.
  String kladrId = null;
  /// Заголовки для работы с API dadata
  Map<String,String> _requestHeaders;
  /// Токен для обращения к дадата
  String token = '405b332ee61c7cfb8f03e19a32e86658d808b603';
  /// Распознанные и сконвертированные города.
  /// Если будут повторы, то берем отсюда.
  Map localityConverterUniq= {};

  convert () async {
    print('получаем кладр айди');
    await _getKladr();
    print('работаем kladrId ${kladrId}');
    int  i = 0;
    // Работаем с задачами
    //_r.table(_tasksTableName).filter(_r.row('id').eq('fc1f7b35-19bc-45e7-ae0a-8ace44e645cf').or(_r.row('id').eq('0142b2a3-9e20-4119-8e28-0c612ac39478')))
    _r.table(_tasksTableName)
        .run(_rethinkConn).then((Cursor taskCursor) async { await for(Map<String, dynamic> taskData in taskCursor) {
      print(i);
      // Список с мапами населенных пунтов. Для замены старого списка.
      List<Map<String, dynamic>> newLocalities =[];
      for (int i=0; i<taskData['localities'].length; i++) {

        var r = await _getDadata(taskData['localities'][i]);
        if (r['suggestions'].length > 1) {
          print ("Много вариантов" );
          print(r['suggestions'].length);
          print(taskData['localities'][i]);
          print(r['suggestions']);
          break;
        } else {
          Map newLocality = dadataToNewStructureForLocality(r['suggestions'][0]);
          newLocalities.add(newLocality);
        }
      }
      Map newAddress = {};
      if ( taskData.containsKey('address') && taskData['address'] != null && (taskData['address'] as Map).isNotEmpty ) {
        if ((taskData['address'] as Map).containsKey('data')) {
          newAddress = dadataToNewStructure(taskData['address']['data']);
          print(taskData['id']);
          print(newAddress);
        }

      }
      // Проверяем, что размер массива нас. пунктов старой структуры совпадает с размером массива новой структуры
      if (newLocalities.length == taskData['localities'].length) {
        writeNewTask(taskData, newLocalities, newAddress);
      }
      i++;
    }});
  }

  _getKladr () async {
    _requestHeaders = {
      "Content-Type": "application/json",
      "Accept": "application/json",
      "Authorization": "Token ${token}"
    };
    /// Получение КЛАДР. Здесь Dadata делает геолокацию по IP адресу.
    http.Request req= new http.Request('get', _detectAddressByIpUrl,headers: _requestHeaders);

    await _client.send(req).then((http.Response response) async {
      Map resBody = await response.readAsString().then((String resBody) => JSON.decode( resBody));
      if (resBody['location']['data'] != null && resBody['location']['data'].containsKey('kladr_id')) {
        kladrId = resBody['location']['data']['kladr_id'];
      } else {
        /// Не удалось определить kladr_id. Больше не пытаемся.
        kladrId = 'undef';
        print("КЛАДР id не удалось определить. Работаем без геопозиционирования");
      }
    });
  }

  Future<Map<String, dynamic>> _getDadata (String localityString) async {
    if (localityConverterUniq.containsKey(localityString))  return localityConverterUniq[localityString];
    print ('не нашли');
    String url = "${_baseSuggestUrl}address";
    Map requestBody = {
      "query": localityString
    };
    // Делаем ограничение выдачи от города до хз. Наверное деревни.
    requestBody["from_bound"] = { "value": "city" };
    requestBody["to_bound"] = { "value": "settlement" };

    http.Request req= new http.Request('post', url, headers: _requestHeaders, body: JSON.encode(requestBody));
    Map dadata = await _client.send(req).then((http.Response response) async {
      Map resBody = JSON.decode(await response.readAsString());
      return resBody;
    });
    localityConverterUniq[localityString] = dadata;
    return dadata;
  }

  /// Из стуктуры, кот. возвращает Datata, формируем структуру новую
  /// Кот. подойдет для населенного пункта
  Map dadataToNewStructureForLocality (Map dadataAddressStructure) {
    Map newLocality = {
      "area": dadataAddressStructure['data']['area'],
      "area_type": dadataAddressStructure['data']['area_type'] ,
      "area_type_full": dadataAddressStructure['data']['area_type_full'] ,
      "city":  dadataAddressStructure['data']['city'] ,
      "city_district": dadataAddressStructure['data']['city_district'] ,
      "city_district_type": dadataAddressStructure['data']['city_district_type'] ,
      "city_district_type_full": dadataAddressStructure['data']['city_district_type_full'] ,
      "city_type":  dadataAddressStructure['data']['city_type'],
      "city_type_full":  dadataAddressStructure['data']['city_type_full'],
      "country":  dadataAddressStructure['data']['country'] ,
      "endpoint_full":  dadataAddressStructure['data']['endpoint_full'] ,
      "endpoint_short":  dadataAddressStructure['data']['endpoint_short'] ,
      "fias_id":  dadataAddressStructure['data']['fias_id'] ,
      "fias_level":  dadataAddressStructure['data']['fias_level'] ,
      "geo_lat":  dadataAddressStructure['data']['geo_lat'] ,
      "geo_lon":  dadataAddressStructure['data']['geo_lon'],
      "kladr_id":  dadataAddressStructure['data']['kladr_id'],
      "qc_geo":  dadataAddressStructure['data']['qc_geo'],
      "region":  dadataAddressStructure['data']['region'],
      "region_type":  dadataAddressStructure['data']['region_type'],
      "region_type_full":  dadataAddressStructure['data']['region_type_full'],
      "settlement": dadataAddressStructure['data']['settlement'] ,
      "settlement_type": dadataAddressStructure['data']['settlement_type'] ,
      "settlement_type_full": dadataAddressStructure['data']['settlement_type_full']
    };
    return newLocality;
  }

  /// Из стуктуры, кот. возвращает Datata формируем структуру новую
  /// Кот. подойдет для адреса
  Map dadataToNewStructure (Map dadataAddressStructure) {
    Map<String, dynamic> curAddress = {
    // Страна
    "country": dadataAddressStructure['country'] ?? null,
    // Регион
    "region_type": dadataAddressStructure['region_type'],
    "region_type_full": dadataAddressStructure['region_type_full'],
    "region": dadataAddressStructure['region'],
    // Район
    "area_type": dadataAddressStructure['area_type'],
    "area_type_full": dadataAddressStructure['area_type_full'],
    "area": dadataAddressStructure['area'],
    // Город
    "city_type": dadataAddressStructure['city_type'],
    "city_type_full": dadataAddressStructure['city_type_full'],
    "city": dadataAddressStructure['city'],
    // Район города
    "city_district_type": dadataAddressStructure['city_district_type'],
    "city_district_type_full": dadataAddressStructure['city_district_type_full'],
    "city_district": dadataAddressStructure['city_district'],
    // Населённый пункт
    "settlement_type": dadataAddressStructure['settlement_type'],
    "settlement_type_full": dadataAddressStructure['settlement_type_full'],
    "settlement": dadataAddressStructure['settlement'],
    // Улица
    "street_type": dadataAddressStructure['street_type'],
    "street_type_full": dadataAddressStructure['street_type_full'],
    "street": dadataAddressStructure['street'],
    // Дом
    "house_type": dadataAddressStructure['house_type'],
    "house_type_full": dadataAddressStructure['house_type_full'],
    "house": dadataAddressStructure['house'],
    // Секция
    "block_type": dadataAddressStructure['block_type'],
    "block_type_full": dadataAddressStructure['block_type_full'],
    "block": dadataAddressStructure['block'],
    // Квартира
    "flat_type": dadataAddressStructure['flat_type'],
    "flat_type_full": dadataAddressStructure['flat_type_full'],
    "flat": dadataAddressStructure['flat'],
    // ФИАС идентификатор
    "fias_id": dadataAddressStructure['fias_id'],
    // Уроверь глубины идентификатора
    "fias_level": dadataAddressStructure['fias_level'],
    // КЛАДР идентификатор
    "kladr_id": dadataAddressStructure['kladr_id'],
    // Координаты
    "geo_lat": dadataAddressStructure['geo_lat'],
    "geo_lon": dadataAddressStructure['geo_lon'],
    // Точность координат
    "qc_geo": dadataAddressStructure['qc_geo'],
    // Итоговая строка полностью
    "endpoint_full": dadataAddressStructure['unrestricted_value'],
    // Города без лишних уточнений, нас пункты с регионом
    "endpoint_short": dadataAddressStructure['city'] == null ?
    dadataAddressStructure['settlement_type'] + ' ' +
        dadataAddressStructure['settlement'] + ' ' +
        dadataAddressStructure['area_type'] + ' ' +
        dadataAddressStructure['area'] : dadataAddressStructure['city']
    };
    return curAddress;
  }

  /// Записывает в задачу новый localities и новый адрес
  /// Два адреса пока быть не может
  void writeNewTask (Map taskData, List<Map> newLocalities, Map newAddress) {
    if (!taskData.containsKey('addresses')) {
      _r.table(_tasksTableName)
          .get(taskData['id'])
          .update({
        "localities": newLocalities,
        "localities_backup": taskData['localities'], // бакапим
        "addresses": [newAddress]
      }).run(_rethinkConn);
    } else {
      print ("уже с обновленной структурой");
    }

  }
}

