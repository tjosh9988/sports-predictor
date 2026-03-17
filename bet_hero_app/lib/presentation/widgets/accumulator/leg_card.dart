import 'package:flutter/material.dart';
import '../../../data/models/accumulator_leg_model.dart';

import '../../../core/theme.dart';
import 'confidence_bar.dart';
import 'odds_display.dart';
import 'status_badge.dart';

class LegCard extends StatefulWidget {
  final AccumulatorLegModel leg;
  final bool showReasoning;

  const LegCard({
    super.key,
    required this.leg,
    this.showReasoning = false,
  });

  @override
  State<LegCard> createState() => _LegCardState();
}

class _LegCardState extends State<LegCard> {
  bool _isExpanded = false;

  @override
  void initState() {
    super.initState();
    _isExpanded = widget.showReasoning;
  }

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.only(bottom: 12),
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: AppTheme.secondaryBackground,
        borderRadius: BorderRadius.circular(12),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Icon(_getSportIcon(widget.leg.sport), size: 16, color: AppTheme.textSecondary),
              const SizedBox(width: 8),
              Text(
                widget.leg.league ?? 'League',
                style: const TextStyle(color: AppTheme.textSecondary, fontSize: 12),
              ),
              const Spacer(),
              StatusBadge(status: widget.leg.status),
            ],
          ),
          const SizedBox(height: 12),
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      '${widget.leg.homeTeam} vs ${widget.leg.awayTeam}',
                      style: const TextStyle(
                        color: AppTheme.textPrimary,
                        fontWeight: FontWeight.bold,
                        fontSize: 14,
                      ),
                    ),
                    const SizedBox(height: 2),
                    Text(
                      widget.leg.league,
                      style: const TextStyle(color: AppTheme.textSecondary, fontSize: 11),
                    ),
                    const SizedBox(height: 6),
                    RichText(
                      text: TextSpan(
                        style: const TextStyle(fontSize: 12),
                        children: [
                          const TextSpan(
                            text: 'Market: ',
                            style: TextStyle(color: AppTheme.primaryGold, fontWeight: FontWeight.w600),
                          ),
                          TextSpan(
                            text: '${widget.leg.market} — ',
                            style: const TextStyle(color: AppTheme.primaryGold, fontWeight: FontWeight.w600),
                          ),
                          TextSpan(
                            text: widget.leg.predictedOutcome,
                            style: const TextStyle(color: AppTheme.textPrimary, fontWeight: FontWeight.bold),
                          ),
                        ],
                      ),
                    ),
                  ],
                ),
              ),
              OddsDisplay(odds: widget.leg.odds, fontSize: 18),
            ],
          ),
          const SizedBox(height: 16),
          Row(
            children: [
              Expanded(child: ConfidenceBar(confidence: widget.leg.confidence)),
              const SizedBox(width: 8),
              Text(
                '${widget.leg.confidence.toStringAsFixed(0)}%',
                style: const TextStyle(color: AppTheme.textPrimary, fontSize: 12, fontWeight: FontWeight.bold),
              ),
              const SizedBox(width: 16),
              if (widget.leg.edge != null)
                Column(
                  crossAxisAlignment: CrossAxisAlignment.end,
                  children: [
                    const Text('Edge', style: TextStyle(color: AppTheme.textSecondary, fontSize: 10)),
                    Text(
                      '+${widget.leg.edge.toStringAsFixed(1)}%',
                      style: const TextStyle(color: AppTheme.successGreen, fontSize: 12, fontWeight: FontWeight.bold),
                    ),
                  ],
                ),
            ],
          ),
          if (widget.leg.aiReasoning != null) ...[
            const SizedBox(height: 8),
            GestureDetector(
              onTap: () => setState(() => _isExpanded = !_isExpanded),
              child: Row(
                children: [
                  const Text('AI Reasoning', style: TextStyle(color: AppTheme.textSecondary, fontSize: 12)),
                  Icon(
                    _isExpanded ? Icons.expand_less : Icons.expand_more,
                    size: 16,
                    color: AppTheme.textSecondary,
                  ),
                ],
              ),
            ),
            if (_isExpanded)
              Padding(
                padding: const EdgeInsets.only(top: 8),
                child: Text(
                  widget.leg.aiReasoning!,
                  style: const TextStyle(
                    color: AppTheme.textSecondary,
                    fontSize: 12,
                    fontStyle: FontStyle.italic,
                  ),
                ),
              ),
          ],
        ],
      ),
    );
  }

  IconData _getSportIcon(String? sport) {
    switch (sport?.toLowerCase()) {
      case 'football': return Icons.sports_soccer;
      case 'basketball': return Icons.sports_basketball;
      case 'tennis': return Icons.sports_tennis;
      case 'nfl': return Icons.sports_football;
      case 'cricket': return Icons.sports_cricket;
      case 'nhl':
      case 'hockey': return Icons.sports_hockey;
      case 'mlb':
      case 'baseball': return Icons.sports_baseball;
      default: return Icons.sports;
    }
  }
}
