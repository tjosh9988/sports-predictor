import 'package:flutter/material.dart';
import '../../../core/theme.dart';


class FormBadge extends StatelessWidget {
  final String result; // 'W', 'D', 'L'

  const FormBadge({super.key, required this.result});

  @override
  Widget build(BuildContext context) {
    Color color;
    switch (result.toUpperCase()) {
      case 'W':
        color = AppTheme.successGreen;
        break;
      case 'D':
        color = Colors.orange;
        break;
      case 'L':
        color = AppTheme.dangerRed;
        break;
      default:
        color = AppTheme.textSecondary;
    }

    return Container(
      width: 20,
      height: 20,
      alignment: Alignment.center,
      decoration: BoxDecoration(
        color: color,
        borderRadius: BorderRadius.circular(4),
      ),
      child: Text(
        result.toUpperCase(),
        style: const TextStyle(
          color: Colors.white,
          fontSize: 10,
          fontWeight: FontWeight.bold,
        ),
      ),
    );
  }
}
