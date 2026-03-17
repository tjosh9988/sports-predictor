import 'package:flutter/material.dart';
import '../../../core/theme.dart';


class LeagueGroupHeader extends StatelessWidget {
  final String leagueName;
  final String? country;

  const LeagueGroupHeader({
    super.key,
    required this.leagueName,
    this.country,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      color: AppTheme.secondaryBackground,
      child: Row(
        children: [
          Icon(Icons.emoji_events_outlined, size: 16, color: AppTheme.primaryGold),
          const SizedBox(width: 8),
          Text(
            leagueName.toUpperCase(),
            style: const TextStyle(
              color: AppTheme.textPrimary,
              fontSize: 12,
              fontWeight: FontWeight.bold,
              letterSpacing: 1.2,
            ),
          ),
          if (country != null) ...[
            const SizedBox(width: 4),
            Text(
              '($country)',
              style: const TextStyle(color: AppTheme.textSecondary, fontSize: 11),
            ),
          ],
        ],
      ),
    );
  }
}
