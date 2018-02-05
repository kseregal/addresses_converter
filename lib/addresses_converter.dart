import 'dart:async';
import 'dart:convert';

import 'package:http_client/console.dart' as http;
import 'package:queries/collections.dart';
import 'package:rethinkdb_driver2/rethinkdb_driver2.dart';

class localityConverter {
  Rethinkdb _r;
  Connection _rethinkConn;
  final Map _config;

  /// Таблица users
  String get _usersTableName => _config["usersTableName"];

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
    // получаем кладр id
    // реально он для этой задачи не используется.
    await _getKladr();

    int  i = 0;
    print ("Обрабатываем пользователей");
    await _r.table(_usersTableName)
        .run(_rethinkConn).then((Cursor usersCursor) async { await for(Map<String, dynamic> userData in usersCursor) {
      //print("${i} id = ${userData['id']}");
      //print(userData);
      // Список с мапами населенных пунтов. Для замены старого списка.
      List<Map<String, dynamic>> newLocalities =[];

      // Список городов для распознования
      List<String> tmpLocalityForRecognition = [];
      if (  (userData['profile'] as Map).containsKey('mainLocality') && userData['profile']['mainLocality'].isNotEmpty ) {
        tmpLocalityForRecognition.add(userData['profile']['mainLocality']);
      }
      if ((userData['profile'] as Map).containsKey('localities')) {
        if (userData['profile']['localities'] is List && userData['profile']['localities'].length > 0) {
          for (int j=0; j < userData['profile']['localities'].length; j++) {
            if (userData['profile']['localities'][j] is String) {
              tmpLocalityForRecognition.add(userData['profile']['localities'][j]);
            }
          }
        }
      }
      // Убираем повторения
      tmpLocalityForRecognition = new Collection(tmpLocalityForRecognition).distinct().toList();

      for (int j=0; j < tmpLocalityForRecognition.length; j++) {
        if (tmpLocalityForRecognition[j] is String) {
          // Найденный Map для населенного пункта
          Map suggest = {};
          if (localityConverterUniq.containsKey(tmpLocalityForRecognition[j])) {
            // нашли в локальном кэше. Используем
            suggest = localityConverterUniq[tmpLocalityForRecognition[j]];
          } else {
            var r = await _getDadata(tmpLocalityForRecognition[j]);
            if (r['suggestions'].length == 0) {
              //print("Dadata ничего не вернул.");

            } else {
              // >=1
              // Если Dadata возвращает несколько вариантов, то берем самый релевантный вариант по мнению Dadata
              //print("${tmpLocalityForRecognition[j]} === ${r['suggestions'][0]['value']}");
              suggest = r['suggestions'][0];
              // пополняем данными локальный кэш населенных пунтов.
              localityConverterUniq[tmpLocalityForRecognition[j]] = suggest;
            }
          }
          Map newLocality = {};
          if (suggest.isNotEmpty) {
            newLocality = dadataToNewStructureForLocality(suggest);
            newLocalities.add(newLocality);
          } else {
            //print('ПЛОХО. НЕ НАШЛИ В ДАДАТА ${userData['id']} ${tmpLocalityForRecognition[j]}');
          }
        } else {
          print("Населенный пункт не строка. Наверное повторный запуск скрипта, и в localities хранится Map.");
        }
      }
      // Проверяем, что размер массива нас. пунктов не 0
      if (newLocalities.length > 0) await _writeNewUser( userData, newLocalities );
      i++;
      //if (i > 1)    break;

    }});

    print("Обработка пользователей по таблице ${_usersTableName} закончена. ${i} записей");

    i = 0;
    // Работаем с задачами
    //_r.table(_tasksTableName).filter(_r.row('id').eq('fc1f7b35-19bc-45e7-ae0a-8ace44e645cf').or(_r.row('id').eq('0142b2a3-9e20-4119-8e28-0c612ac39478')))
    await _r.table(_tasksTableName)
        .run(_rethinkConn).then((Cursor taskCursor) async { await for(Map<String, dynamic> taskData in taskCursor) {
      //print("${i} id = ${taskData['id']}");
      // Список с мапами населенных пунтов. Для замены старого списка.
      List<Map<String, dynamic>> newLocalities =[];
      for (int j=0; j<taskData['localities'].length; j++) {
        Map suggest = {};
        if (taskData['localities'][j] is String) {
          if (localityConverterUniq.containsKey(taskData['localities'][j])) {
            // нашли в локальном кэше. Используем
            suggest = localityConverterUniq[taskData['localities'][j]];
          } else {
            var r = await _getDadata(taskData['localities'][j]);
            if (r['suggestions'].length > 0) {
              // Если Dadata возвращает несколько вариантов, то берем самый релевантный вариант по мнению Dadata
              suggest = r['suggestions'][0];
              localityConverterUniq[taskData['localities'][j]] = suggest;
            }
          }
          if (suggest.isNotEmpty) { // >=1
            // print("${taskData['localities'][j]} === ${r['suggestions'][0]['value']}");
            Map newLocality = dadataToNewStructureForLocality( suggest );
            newLocalities.add(newLocality);
          } else {
            //print('ПЛОХО. НЕ НАШЛИ В ДАДАТА ${taskData['id']} ${taskData['localities'][j]}');
          }
        } else {
          print("Населенный пункт не строка. Наверное уже обработали, и в localities хранится как List<Map>.");
        }
      }

      // Обрабатываем адрес.
      // Сейчас хранится один адрес.
      // Его нужно только слегка причесать.
      // Новый мап адреса для списка tasks.addresses
      Map newAddress = {};
      if ( taskData.containsKey('address') && taskData['address'] != null && (taskData['address'] as Map).isNotEmpty ) {
        if ((taskData['address'] as Map).containsKey('data')) {
          newAddress = dadataToNewStructure(taskData['address']['data']);
          //print(taskData['id']);
          //print(newAddress);
        }
      }
      // Проверяем, что размер массива нас. пунктов старой структуры совпадает с размером массива новой структуры
      if (newLocalities.length == taskData['localities'].length) {
        await _writeNewTask(taskData, newLocalities, newAddress);
      }
      i++;
      //if (i > 1)    break;
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

  /// Получаем из Dadata подсказки по названию населенного пункта.
  Future<Map<String, dynamic>> _getDadata (String localityString) async {

    String url = "${_baseSuggestUrl}address";
    Map requestBody = {
      "query": localityString
    };
    // Делаем ограничение выдачи от города до хз. Наверное деревни.
    requestBody["from_bound"] = { "value": "city" };
    requestBody["to_bound"] = { "value": "city" };
    //requestBody["to_bound"] = { "value": "settlement" };

    http.Request req= new http.Request('post', url, headers: _requestHeaders, body: JSON.encode(requestBody));
    Map dadata = await _client.send(req).then((http.Response response) async {
      Map resBody = JSON.decode(await response.readAsString());
      return resBody;
    });

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
  Future _writeNewTask (Map taskData, List<Map> newLocalities, Map newAddress) async {
    if (!taskData.containsKey('addresses')) {
      return await _r.table(_tasksTableName)
          .get(taskData['id'])
          .update({
        "localities": newLocalities,
        "localities_backup": taskData['localities'], // бакапим
        "addresses": [newAddress]
      }).run(_rethinkConn);
    } else {
      //print ("уже с обновленной структурой");
    }

  }
  /// Записывает в пользователя новый localities и новый адрес
  /// Два адреса пока быть не может
  Future _writeNewUser (Map userData, List<Map> newLocalities) async {
    return await _r.table(_usersTableName)
        .get( userData['id'] )
        .update({"profile": {
      "localities": newLocalities,
      "localities_backup": userData['profile']['localities'], // бакапим
    }}).run(_rethinkConn);
  }
}


