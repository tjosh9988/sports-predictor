import 'package:flutter/material.dart';
import '../../../core/theme.dart';

class ConfidenceBar extends StatelessWidget {
  final double confidence; // 0.0 to 1.0

  const ConfidenceBar({super.key, required this.confidence});

  @override
  Widget build(BuildContext context) {
    Color barColor;
    if (confidence >= 70) {
      barColor = AppTheme.successGreen;
    } else if (confidence >= 55) {
      barColor = AppTheme.primaryGold;
    } else {
      barColor = Colors.orange;
    }

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            const Text(
              'Confidence',
              style: TextStyle(color: AppTheme.textSecondary, fontSize: 10),
            ),
            Text(
              '${confidence.toStringAsFixed(0)}%',
              style: TextStyle(
                color: barColor,
                fontSize: 10,
                fontWeight: FontWeight.bold,
              ),
            ),
          ],
        ),
        const SizedBox(height: 4),
        ClipRRect(
          borderRadius: BorderRadius.circular(2),
          child: LinearProgressIndicator(
            value: confidence / 100,
            backgroundColor: Colors.white.withOpacity(0.05),
            valueColor: AlwaysStoppedAnimation<Color>(barColor),
            minHeight: 4,
          ),
        ),
      ],
    );
  }
}
