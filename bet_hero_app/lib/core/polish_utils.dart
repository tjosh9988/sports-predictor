import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:connectivity_plus/connectivity_plus.dart';
import '../core/theme.dart';

class PolishUtils {
  static void hapticSuccess() {
    HapticFeedback.mediumImpact();
  }

  static void hapticSelection() {
    HapticFeedback.selectionClick();
  }

  static Future<bool> hasInternet() async {
    final result = await Connectivity().checkConnectivity();
    return result != ConnectivityResult.none;
  }

  static void showOfflineBanner(BuildContext context) {
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(
        content: Row(
          children: [
            Icon(Icons.wifi_off, color: Colors.white, size: 18),
            SizedBox(width: 8),
            Text('No internet connection. Showing cached data.'),
          ],
        ),
        backgroundColor: AppTheme.dangerRed,
        duration: Duration(seconds: 5),
      ),
    );
  }
}
