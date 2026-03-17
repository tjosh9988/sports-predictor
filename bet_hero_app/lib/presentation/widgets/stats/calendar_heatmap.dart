import 'package:flutter/material.dart';
import '../../../core/theme.dart';


class CalendarHeatmap extends StatelessWidget {
  final Map<int, String> results; // Day -> 'won' or 'lost'

  const CalendarHeatmap({super.key, required this.results});

  @override
  Widget build(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        const Text(
          'Performance Calendar',
          style: TextStyle(fontWeight: FontWeight.bold, fontSize: 14),
        ),
        const SizedBox(height: 12),
        GridView.builder(
          shrinkWrap: true,
          physics: const NeverScrollableScrollPhysics(),
          itemCount: 31,
          gridDelegate: const SliverGridDelegateWithFixedCrossAxisCount(
            crossAxisCount: 7,
            mainAxisSpacing: 4,
            crossAxisSpacing: 4,
          ),
          itemBuilder: (context, index) {
            final day = index + 1;
            final status = results[day];
            Color color = Colors.white.withOpacity(0.05);
            if (status == 'won') color = AppTheme.successGreen.withOpacity(0.6);
            if (status == 'lost') color = AppTheme.dangerRed.withOpacity(0.6);

            return Container(
              alignment: Alignment.center,
              decoration: BoxDecoration(
                color: color,
                borderRadius: BorderRadius.circular(4),
              ),
              child: Text(
                day.toString(),
                style: const TextStyle(fontSize: 10, color: AppTheme.textPrimary),
              ),
            );
          },
        ),
        const SizedBox(height: 8),
        Row(
          mainAxisAlignment: MainAxisAlignment.end,
          children: [
            _legendItem('Winning Day', AppTheme.successGreen),
            const SizedBox(width: 12),
            _legendItem('Losing Day', AppTheme.dangerRed),
          ],
        ),
      ],
    );
  }

  Widget _legendItem(String label, Color color) {
    return Row(
      children: [
        Container(width: 8, height: 8, decoration: BoxDecoration(color: color.withOpacity(0.6), borderRadius: BorderRadius.circular(2))),
        const SizedBox(width: 4),
        Text(label, style: const TextStyle(color: AppTheme.textSecondary, fontSize: 8)),
      ],
    );
  }
}
