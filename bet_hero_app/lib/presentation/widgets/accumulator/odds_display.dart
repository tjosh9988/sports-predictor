import 'package:flutter/material.dart';
import '../../../core/theme.dart';

class OddsDisplay extends StatelessWidget {
  final double odds;
  final double? previousOdds;
  final double fontSize;

  const OddsDisplay({
    super.key,
    required this.odds,
    this.previousOdds,
    this.fontSize = 16,
  });

  @override
  Widget build(BuildContext context) {
    bool hasIncreased = previousOdds != null && odds > previousOdds!;
    bool hasDecreased = previousOdds != null && odds < previousOdds!;

    return Row(
      mainAxisSize: MainAxisSize.min,
      children: [
        Text(
          odds.toStringAsFixed(2),
          style: TextStyle(
            color: AppTheme.primaryGold,
            fontSize: fontSize,
            fontWeight: FontWeight.bold,
          ),
        ),
        if (hasIncreased)
          const Icon(Icons.arrow_drop_up, color: AppTheme.successGreen, size: 16),
        if (hasDecreased)
          const Icon(Icons.arrow_drop_down, color: AppTheme.dangerRed, size: 16),
      ],
    );
  }
}
