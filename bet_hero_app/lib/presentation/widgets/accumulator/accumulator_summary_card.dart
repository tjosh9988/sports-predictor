import 'package:flutter/cupertino.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import '../../../data/models/accumulator_model.dart';
import '../../../core/theme.dart';
import '../../../core/polish_utils.dart';
import 'status_badge.dart';
import 'odds_display.dart';

class AccumulatorSummaryCard extends StatelessWidget {
  final AccumulatorModel accumulator;
  final VoidCallback onTap;
  final int index;

  const AccumulatorSummaryCard({
    super.key,
    required this.accumulator,
    required this.onTap,
    this.index = 0,
  });

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: () {
        PolishUtils.hapticSelection();
        onTap();
      },
      child: Hero(
        tag: 'acca_${accumulator.id}_$index',
        child: Container(
          padding: const EdgeInsets.all(12),
          decoration: BoxDecoration(
            color: AppTheme.cardBackground,
            borderRadius: BorderRadius.circular(12),
            border: Border.all(color: Colors.white.withOpacity(0.05)),
          ),
          child: Material(
            color: Colors.transparent,
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  children: [
                    Text(
                      accumulator.type.value.toUpperCase(),
                      style: const TextStyle(
                        color: AppTheme.textPrimary,
                        fontSize: 12,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    StatusBadge(status: accumulator.status.value, compact: true),
                  ],
                ),
                const Spacer(),
                OddsDisplay(odds: accumulator.totalOdds, fontSize: 20),
                Text(
                  '${accumulator.legs.length} LEGS',
                  style: const TextStyle(color: AppTheme.textSecondary, fontSize: 10),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}
