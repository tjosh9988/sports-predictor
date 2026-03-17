import 'package:flutter/material.dart';
import '../../../core/theme.dart';


class StatComparisonRow extends StatelessWidget {
  final String label;
  final String homeValue;
  final String awayValue;
  final double? homeRatio; // 0.0 to 1.0 for the bar visualization

  const StatComparisonRow({
    super.key,
    required this.label,
    required this.homeValue,
    required this.awayValue,
    this.homeRatio,
  });

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 12.0),
      child: Column(
        children: [
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Text(homeValue, style: const TextStyle(fontWeight: FontWeight.bold, color: AppTheme.textPrimary)),
              Text(label, style: const TextStyle(color: AppTheme.textSecondary, fontSize: 12)),
              Text(awayValue, style: const TextStyle(fontWeight: FontWeight.bold, color: AppTheme.textPrimary)),
            ],
          ),
          const SizedBox(height: 8),
          Row(
            children: [
              Expanded(
                child: RotatedBox(
                  quarterTurns: 2,
                  child: LinearProgressIndicator(
                    value: homeRatio ?? 0.5,
                    backgroundColor: Colors.white.withOpacity(0.05),
                    valueColor: const AlwaysStoppedAnimation<Color>(AppTheme.primaryGold),
                    minHeight: 4,
                  ),
                ),
              ),
              const SizedBox(width: 4),
              Expanded(
                child: LinearProgressIndicator(
                  value: 1 - (homeRatio ?? 0.5),
                  backgroundColor: Colors.white.withOpacity(0.05),
                  valueColor: const AlwaysStoppedAnimation<Color>(AppTheme.textSecondary),
                  minHeight: 4,
                ),
              ),
            ],
          ),
        ],
      ),
    );
  }
}
