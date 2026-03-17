import 'package:flutter/material.dart';
import '../../../core/polish_utils.dart';
import '../../../core/theme.dart';
import '../../../data/models/fixture_model.dart';


class FixtureCard extends StatelessWidget {
  final FixtureModel fixture;
  final VoidCallback onTap;

  const FixtureCard({
    super.key,
    required this.fixture,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: () {
        PolishUtils.hapticSelection();
        onTap();
      },
      child: Hero(
        tag: 'fixture_${fixture.id}',
        child: Container(
          padding: const EdgeInsets.all(16),
          decoration: BoxDecoration(
            color: Colors.transparent, // Maintain background for gesture
            border: Border(bottom: BorderSide(color: Colors.white.withOpacity(0.05))),
          ),
          child: Material(
            color: Colors.transparent,
            child: Column(
              children: [
                Row(
                  children: [
                    Expanded(
                      child: Text(
                        fixture.homeTeam,
                        textAlign: TextAlign.end,
                        style: const TextStyle(fontWeight: FontWeight.w600, color: AppTheme.textPrimary),
                      ),
                    ),
                    Container(
                      width: 60,
                      alignment: Alignment.center,
                      child: Column(
                        children: [
                          Text(
                            _getScoreOrTime(),
                            style: TextStyle(
                              color: fixture.status == 'LIVE' ? AppTheme.successGreen : AppTheme.primaryGold,
                              fontWeight: FontWeight.bold,
                            ),
                          ),
                          if (fixture.status == 'LIVE')
                            const Text('LIVE', style: TextStyle(color: AppTheme.successGreen, fontSize: 8)),
                        ],
                      ),
                    ),
                    Expanded(
                      child: Text(
                        fixture.awayTeam,
                        style: const TextStyle(fontWeight: FontWeight.w600, color: AppTheme.textPrimary),
                      ),
                    ),
                  ],
                ),
                if (fixture.homeOdds != null) ...[
                  const SizedBox(height: 12),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      _oddsBox('1', fixture.homeOdds!),
                      const SizedBox(width: 8),
                      _oddsBox('X', fixture.drawOdds ?? 0),
                      const SizedBox(width: 8),
                      _oddsBox('2', fixture.awayOdds!),
                    ],
                  ),
                ],
                if (fixture.predictionId != null) ...[
                  const SizedBox(height: 8),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      const Icon(Icons.psychology, size: 14, color: AppTheme.primaryGold),
                      const SizedBox(width: 4),
                      Text(
                        'AI PREDICTION ACTIVE',
                        style: TextStyle(color: AppTheme.primaryGold, fontSize: 10, fontWeight: FontWeight.bold),
                      ),
                    ],
                  ),
                ],
              ],
            ),
          ),
        ),
      ),
    );
  }

  Widget _oddsBox(String label, double val) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 4),
      decoration: BoxDecoration(
        color: AppTheme.secondaryBackground,
        borderRadius: BorderRadius.circular(4),
      ),
      child: Row(
        children: [
          Text(label, style: const TextStyle(color: AppTheme.textSecondary, fontSize: 10)),
          const SizedBox(width: 6),
          Text(
            val.toStringAsFixed(2),
            style: const TextStyle(color: AppTheme.textPrimary, fontSize: 12, fontWeight: FontWeight.bold),
          ),
        ],
      ),
    );
  }

  String _getScoreOrTime() {
    if (fixture.homeScore != null && fixture.awayScore != null) {
      return '${fixture.homeScore} - ${fixture.awayScore}';
    }
    return fixture.matchDate.toLocal().toString().substring(11, 16);
  }
}
