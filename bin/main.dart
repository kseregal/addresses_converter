import 'dart:io';
import 'package:rethinkdb_driver2/rethinkdb_driver2.dart';
import 'package:addresses_converter/addresses_converter.dart';
import 'package:yaml/yaml.dart';

main(List<String> arguments) async {
  Map<String, dynamic> config = await loadYaml(new File('config.yaml').readAsStringSync());
  Map<String, dynamic> _rethink;
  /// Объект управления  соединениями с базой RethinkDB
  Rethinkdb _r;
  /// Соединение с базой RethinkDb
  Connection _rethinkConn;
  /// Создаем соединение с базой данных RethinkDb.
  _r = new Rethinkdb();
  _rethinkConn = await _r.connect(
      db: config["rethink"]["dbName"],
      host: config["rethink"]["host"],
      port: config["rethink"]["port"]
  );
  localityConverter locConv = new localityConverter(_r, _rethinkConn, config);
  locConv.convert();

}