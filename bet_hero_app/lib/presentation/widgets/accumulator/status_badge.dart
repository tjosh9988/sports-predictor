import 'package:flutter/material.dart';
import '../../../core/theme.dart';

class StatusBadge extends StatelessWidget {
  final String status;
  final bool compact;

  const StatusBadge({
    super.key,
    required this.status,
    this.compact = false,
  });

  @override
  Widget build(BuildContext context) {
    Color color;
    IconData icon;
    String label = status.toUpperCase();

    switch (status.toLowerCase()) {
      case 'won':
      case 'correct':
      case 'success':
        color = AppTheme.successGreen;
        icon = Icons.check_circle_outline;
        break;
      case 'lost':
      case 'incorrect':
      case 'failed':
        color = AppTheme.dangerRed;
        icon = Icons.highlight_off;
        break;
      case 'pending':
      case 'upcoming':
        color = AppTheme.primaryGold;
        icon = Icons.schedule;
        break;
      case 'ready':
        color = AppTheme.primaryGold;
        icon = Icons.bolt;
        break;
      default:
        color = AppTheme.textSecondary;
        icon = Icons.help_outline;
    }

    if (compact) {
      return Icon(icon, color: color, size: 16);
    }

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
      decoration: BoxDecoration(
        color: color.withOpacity(0.1),
        borderRadius: BorderRadius.circular(4),
        border: Border(
          bottom: BorderSide(
            color: Colors.white.withValues(alpha: 0.05),
            width: 1.0,
          ),
        ),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, color: color, size: 14),
          const SizedBox(width: 4),
          Text(
            label,
            style: TextStyle(
              color: color,
              fontSize: 10,
              fontWeight: FontWeight.bold,
            ),
          ),
        ],
      ),
    );
  }
}
