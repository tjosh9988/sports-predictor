import 'package:flutter/material.dart';
import '../../../core/theme.dart';
import '../accumulator/status_badge.dart';

class AccumulatorHistoryCard extends StatelessWidget {
  final String id;
  final int index;
  final String date;
  final String type;
  final double odds;
  final String status;
  final VoidCallback onTap;

  const AccumulatorHistoryCard({
    super.key,
    required this.id,
    required this.index,
    required this.date,
    required this.type,
    required this.odds,
    required this.status,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: onTap,
      child: Hero(
        tag: 'acca_${id}_$index',
        child: Material(
          color: Colors.transparent,
          child: Container(
            margin: const EdgeInsets.only(bottom: 12),
            padding: const EdgeInsets.all(16),
            decoration: BoxDecoration(
              color: AppTheme.cardBackground,
              borderRadius: BorderRadius.circular(12),
            ),
            child: Row(
              children: [
                Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(date, style: const TextStyle(color: AppTheme.textSecondary, fontSize: 10)),
                    const SizedBox(height: 4),
                    Text(
                      type.toUpperCase(),
                      style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 14),
                    ),
                  ],
                ),
                const Spacer(),
                Column(
                  crossAxisAlignment: CrossAxisAlignment.end,
                  children: [
                    Text(
                      '${odds.toStringAsFixed(2)} ODDS',
                      style: const TextStyle(color: AppTheme.primaryGold, fontWeight: FontWeight.bold, fontSize: 14),
                    ),
                    const SizedBox(height: 4),
                    StatusBadge(status: status, compact: false),
                  ],
                ),
                const SizedBox(width: 12),
                const Icon(Icons.chevron_right, color: AppTheme.textSecondary),
              ],
            ),
          ),
        ),
      ),
    );
  }
}
