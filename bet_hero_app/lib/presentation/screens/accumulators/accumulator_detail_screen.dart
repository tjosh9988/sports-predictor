import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../../../core/theme.dart';
import '../../../data/models/accumulator_model.dart';

import '../../providers/accumulator_provider.dart';
import '../../widgets/accumulator/leg_card.dart';
import '../../widgets/accumulator/stake_calculator.dart';
import '../../widgets/accumulator/status_badge.dart';

class AccumulatorDetailScreen extends ConsumerWidget {
  final String type;
  final int index;

  const AccumulatorDetailScreen({
    super.key, 
    required this.type,
    this.index = 0,
  });

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final accaType = AccaType.fromString(type);
    final accaAsync = ref.watch(accumulatorByTypeProvider(accaType));

    return Scaffold(
      appBar: AppBar(
        title: Text('${type.toUpperCase()} COMBO'),
      ),
      body: accaAsync.when(
        data: (acca) {
          if (acca == null) return const Center(child: Text('Accumulator not found'));
          return SingleChildScrollView(
            padding: const EdgeInsets.all(16.0),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                _buildHeader(acca),
                const SizedBox(height: 24),
                StakeCalculator(totalOdds: acca.totalOdds),
                const SizedBox(height: 32),
                const Text('SELECTIONS', style: TextStyle(fontWeight: FontWeight.bold, letterSpacing: 1)),
                const SizedBox(height: 16),
                ...acca.legs.map((leg) => LegCard(leg: leg, showReasoning: true)),
                const SizedBox(height: 32),
                _buildFooter(),
                const SizedBox(height: 48),
              ],
            ),
          );
        },
        loading: () => const Center(child: CircularProgressIndicator()),
        error: (e, _) => Center(child: Text('Error: $e')),
      ),
    );
  }

  Widget _buildHeader(dynamic acca) {
    return Hero(
      tag: 'acca_${acca.id}_$index',
      child: Material(
        color: Colors.transparent,
        child: Column(
          children: [
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    const Text('Combined Odds', style: TextStyle(color: AppTheme.textSecondary)),
                    Text(
                      acca.totalOdds.toStringAsFixed(2),
                      style: const TextStyle(
                        color: AppTheme.primaryGold,
                        fontSize: 40,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                  ],
                ),
                StatusBadge(status: acca.status.value),
              ],
            ),
            const SizedBox(height: 16),
            Container(
              padding: const EdgeInsets.all(12),
              decoration: BoxDecoration(
                color: AppTheme.primaryGold.withOpacity(0.1),
                borderRadius: BorderRadius.circular(8),
              ),
              child: Row(
                children: [
                  const Icon(Icons.verified_user, color: AppTheme.primaryGold, size: 20),
                  const SizedBox(width: 12),
                  Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text('AI CONFIDENCE', style: TextStyle(color: AppTheme.textSecondary, fontSize: 10)),
                      Text(
                        '${acca.confidenceScore.toStringAsFixed(1)}%',
                        style: const TextStyle(color: AppTheme.primaryGold, fontWeight: FontWeight.bold),
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildFooter() {
    return Column(
      children: [
        SizedBox(
          width: double.infinity,
          height: 52,
          child: ElevatedButton.icon(
            onPressed: () {},
            icon: const Icon(Icons.add_shopping_cart),
            label: const Text('Add to Betslip'),
          ),
        ),
        const SizedBox(height: 12),
        SizedBox(
          width: double.infinity,
          height: 52,
          child: OutlinedButton.icon(
            onPressed: () {},
            icon: const Icon(Icons.share),
            label: const Text('Share Accumulator'),
          ),
        ),
        const SizedBox(height: 24),
        const Text(
          'Disclaimer: Predictions are based on AI analysis and do not guarantee results. Gamble responsibly.',
          textAlign: TextAlign.center,
          style: TextStyle(color: AppTheme.textSecondary, fontSize: 11),
        ),
      ],
    );
  }
}
