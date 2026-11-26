import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:ftpconnect/ftpconnect.dart';
import 'package:test/test.dart';

void main() {
  test('FTP text callbacks cover commands, replies and a whole listing',
      () async {
    const filename = 'café.txt';
    const directory = '/café';
    const listing =
        'type=file;size=3; café.txt\r\ntype=file;size=1; thé.txt\r\n';
    const contents = [0, 233, 255];
    final commands = <String>[];
    final encodedCommands = <String>[];
    final decodedText = <String>[];
    final uploaded = Completer<List<int>>();
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(server.close);
    server.listen((control) async {
      ServerSocket? dataServer;
      Future<Socket>? dataConnection;
      void reply(String text) => control.add(latin1.encode('$text\r\n'));

      reply('220-test server');
      reply('220 prêt');
      try {
        await for (final command
            in latin1.decoder.bind(control).transform(const LineSplitter())) {
          commands.add(command);
          if (command == 'PASV') {
            final passive =
                await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
            dataServer = passive;
            dataConnection = passive.first;
            reply(
                '227 Passive (127,0,0,1,${passive.port ~/ 256},${passive.port % 256})');
            continue;
          }
          switch (command) {
            case 'USER anonymous':
              reply('230 Logged in');
            case 'CWD $directory':
              reply('250 Directory changed');
            case 'PWD':
              reply('257 "$directory"');
            case 'mlsd':
              final data = await dataConnection!;
              reply('150 Opening data connection');
              data.add(latin1.encode(listing));
              await data.close();
              reply('226 Transfer complete');
              await dataServer!.close();
            case 'SIZE $filename':
              reply('213 3');
            case 'RETR $filename':
              final data = await dataConnection!;
              reply('150 Opening data connection');
              data.add(contents);
              await data.close();
              reply('226 Transfer complete');
              await dataServer!.close();
            case 'STOR $filename':
              final data = await dataConnection!;
              reply('150 Opening data connection');
              uploaded.complete(await data.expand((chunk) => chunk).toList());
              data.destroy();
              reply('226 Transfer complete');
              await dataServer!.close();
            case 'QUIT':
              reply('221 Goodbye');
              await control.flush();
              return;
            default:
              reply('500 Unexpected command');
          }
        }
      } finally {
        await dataServer?.close();
        control.destroy();
      }
    });

    final ftp = FTPConnect(
      '127.0.0.1',
      port: server.port,
      timeout: 3,
      encodeCommand: (command) async {
        encodedCommands.add(command);
        return latin1.encode(command);
      },
      decodeResponse: (bytes) async {
        final text = latin1.decode(bytes);
        decodedText.add(text);
        return text;
      },
    );
    expect(await ftp.connect(), isTrue);
    addTearDown(ftp.disconnect);

    expect(await ftp.changeDirectory(directory), isTrue);
    expect(await ftp.currentDirectory(), directory);
    expect(
      (await ftp.listDirectoryContent()).map((entry) => entry.name),
      [filename, 'thé.txt'],
    );
    expect(await ftp.downloadToBytes(filename), contents);
    expect(
        await ftp.uploadData(Uint8List.fromList(contents), filename), isTrue);
    expect(await uploaded.future, contents);
    expect(encodedCommands, commands);
    expect(decodedText.where((text) => text == listing), hasLength(1));
    expect(decodedText, contains('220-test server\r\n220 prêt\r\n'));
  });
}
