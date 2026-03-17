import 'dart:convert';
import 'package:firebase_messaging/firebase_messaging.dart';
import 'package:flutter_local_notifications/flutter_local_notifications.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../core/router.dart';

/// Top-level background message handler for Firebase Messaging.
@pragma('vm:entry-point')
Future<void> _firebaseMessagingBackgroundHandler(RemoteMessage message) async {
  // Ensure Firebase is initialized if you need to call any Firebase services here
  print("Background message received: ${message.messageId}");
}

final notificationServiceProvider = Provider((ref) => NotificationService());

class NotificationService {
  final FirebaseMessaging _fcm = FirebaseMessaging.instance;
  final FlutterLocalNotificationsPlugin _localNotifications = FlutterLocalNotificationsPlugin();

  Future<void> initialize() async {
    // 1. Request and Check Permissions
    await _fcm.requestPermission(
      alert: true,
      badge: true,
      sound: true,
    );

    // 2. Setup Background Handler
    FirebaseMessaging.onBackgroundMessage(_firebaseMessagingBackgroundHandler);

    // 3. Initialize Local Notifications
    const AndroidInitializationSettings androidSettings = AndroidInitializationSettings('@mipmap/ic_launcher');
    const DarwinInitializationSettings iosSettings = DarwinInitializationSettings();
    const InitializationSettings initSettings = InitializationSettings(android: androidSettings, iOS: iosSettings);

    await _localNotifications.initialize(
      settings: initSettings,
      onDidReceiveNotificationResponse: (NotificationResponse response) {
        if (response.payload != null) {
          _handleDeepLink(response.payload!);
        }
      },
    );

    // 4. Foreground Message Listener
    FirebaseMessaging.onMessage.listen((RemoteMessage message) {
      _showNotification(message);
    });

    // 5. App in Background (Not Terminated) Tap Listener
    FirebaseMessaging.onMessageOpenedApp.listen((RemoteMessage message) {
      _processDataNavigation(message.data);
    });

    // 6. Terminated State Launch Listener
    RemoteMessage? initialMessage = await _fcm.getInitialMessage();
    if (initialMessage != null) {
      _processDataNavigation(initialMessage.data);
    }
  }

  Future<void> _showNotification(RemoteMessage message) async {
    final notification = message.notification;
    if (notification == null) return;

    // Generate a unique integer ID
    // Priority: 'id' from data > messageId hash > timestamp
    int id = 0;
    if (message.data.containsKey('id')) {
      id = int.tryParse(message.data['id'].toString()) ?? message.data['id'].hashCode;
    } else if (message.messageId != null) {
      id = message.messageId.hashCode;
    } else {
      id = DateTime.now().millisecondsSinceEpoch ~/ 1000;
    }

    const AndroidNotificationDetails androidDetails = AndroidNotificationDetails(
      'bet_hero_alerts',
      'Predictions & Alerts',
      channelDescription: 'Real-time AI sport betting predictions',
      importance: Importance.max,
      priority: Priority.high,
    );

    const NotificationDetails details = NotificationDetails(
      android: androidDetails,
      iOS: DarwinNotificationDetails(),
    );

    await _localNotifications.show(
      id: id,
      title: notification.title,
      body: notification.body,
      notificationDetails: details,
      payload: jsonEncode(message.data),
    );
  }

  void _processDataNavigation(Map<String, dynamic> data) {
    if (data.containsKey('screen')) {
      _handleDeepLink(jsonEncode(data));
    }
  }

  void _handleDeepLink(String payload) {
    try {
      final Map<String, dynamic> data = jsonDecode(payload);
      final String? screen = data['screen'];
      final String? id = data['id'];

      if (screen == null) return;

      switch (screen) {
        case 'accumulator':
          if (id != null) {
            AppRouter.push('/accumulator/$id');
          } else {
            AppRouter.push('/home/accumulators');
          }
          break;
        case 'fixture':
          if (id != null) {
            AppRouter.push('/fixture/$id');
          } else {
            AppRouter.push('/home/fixtures');
          }
          break;
        case 'results':
          AppRouter.push('/home/results');
          break;
        case 'stats':
          AppRouter.push('/home/stats');
          break;
        default:
          AppRouter.push('/notifications');
          break;
      }
    } catch (e) {
      print('Deep Link Error: $e');
    }
  }

  Future<String?> getPushToken() async {
    return await _fcm.getToken();
  }
}
