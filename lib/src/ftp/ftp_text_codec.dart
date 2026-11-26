import 'dart:async';
import 'dart:typed_data';

/// Encodes an FTP control command without the terminating CRLF.
typedef FtpCommandEncoder = FutureOr<List<int>> Function(String command);

/// Decodes a complete FTP control reply or a complete directory listing.
typedef FtpResponseDecoder = FutureOr<String> Function(Uint8List bytes);
