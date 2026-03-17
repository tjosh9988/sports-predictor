import 'package:flutter/material.dart';
import '../../../core/theme.dart';


class ProbabilityBar extends StatelessWidget {
  final String label;
  final double probability;
  final double? bookmakerOdds;
  final double? modelOdds;

  const ProbabilityBar({
    super.key,
    required this.label,
    required this.probability,
    this.bookmakerOdds,
    this.modelOdds,
  });

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 8.0),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Text(
                label,
                style: const TextStyle(color: AppTheme.textPrimary, fontSize: 13, fontWeight: FontWeight.w600),
              ),
              Text(
                '${probability.toStringAsFixed(1)}%',
                style: const TextStyle(color: AppTheme.primaryGold, fontSize: 13, fontWeight: FontWeight.bold),
              ),
            ],
          ),
          const SizedBox(height: 6),
          ClipRRect(
            borderRadius: BorderRadius.circular(4),
            child: LinearProgressIndicator(
              value: probability,
              minHeight: 8,
              backgroundColor: Colors.white.withOpacity(0.05),
              valueColor: const AlwaysStoppedAnimation<Color>(AppTheme.primaryGold),
            ),
          ),
          if (bookmakerOdds != null && modelOdds != null) ...[
            const SizedBox(height: 4),
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Text(
                  'Model Odds: ${modelOdds!.toStringAsFixed(2)}',
                  style: const TextStyle(color: AppTheme.textSecondary, fontSize: 10),
                ),
                Text(
                  'Bookie Odds: ${bookmakerOdds!.toStringAsFixed(2)}',
                  style: const TextStyle(color: AppTheme.textSecondary, fontSize: 10),
                ),
              ],
            ),
          ],
        ],
      ),
    );
  }
}
