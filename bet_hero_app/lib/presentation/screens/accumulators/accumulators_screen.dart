import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../../../core/theme.dart';
import '../../../data/models/accumulator_model.dart';

import '../../providers/accumulator_provider.dart';
import '../../widgets/accumulator/leg_card.dart';
import '../../widgets/accumulator/status_badge.dart';


class AccumulatorsScreen extends ConsumerWidget {
  const AccumulatorsScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return DefaultTabController(
      length: 3,
      initialIndex: 0,
      child: Scaffold(
        appBar: AppBar(
          title: const Text('ACCUMULATORS'),
          bottom: const TabBar(
            tabs: [
              Tab(text: '10 ODDS'),
              Tab(text: '5 ODDS'),
              Tab(text: '3 ODDS'),
            ],
            indicatorColor: AppTheme.primaryGold,
            labelColor: AppTheme.primaryGold,
            unselectedLabelColor: AppTheme.textSecondary,
          ),
        ),
        body: TabBarView(
          children: [
            _buildAccaView(ref, AccaType.ten),
            _buildAccaView(ref, AccaType.five),
            _buildAccaView(ref, AccaType.three),
          ],
        ),
      ),
    );
  }

  Widget _buildAccaView(WidgetRef ref, AccaType type) {
    final accaAsync = ref.watch(accumulatorByTypeProvider(type));

    return RefreshIndicator(
      onRefresh: () async => ref.invalidate(accumulatorByTypeProvider(type)),
      child: accaAsync.when(
        data: (acca) {
          if (acca == null) return const Center(child: Text('No predictions available for this type.'));
          return ListView(
            padding: const EdgeInsets.all(16),
            children: [
              _buildStatsHeader(acca),
              const SizedBox(height: 24),
              const Text('SELECTIONS', style: TextStyle(fontWeight: FontWeight.bold, fontSize: 14)),
              const SizedBox(height: 16),
              ...acca.legs.map((leg) => LegCard(leg: leg)),
              const SizedBox(height: 100),
            ],
          );
        },
        loading: () => const Center(child: CircularProgressIndicator()),
        error: (e, _) => Center(child: Text('Error: $e')),
      ),
    );
  }

  Widget _buildStatsHeader(dynamic acca) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: AppTheme.cardBackground,
        borderRadius: BorderRadius.circular(12),
      ),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              const Text('Total Odds', style: TextStyle(color: AppTheme.textSecondary, fontSize: 12)),
              Text(
                acca.totalOdds.toStringAsFixed(2),
                style: const TextStyle(color: AppTheme.primaryGold, fontSize: 24, fontWeight: FontWeight.bold),
              ),
            ],
          ),
          Column(
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              const Text('Confidence', style: TextStyle(color: AppTheme.textSecondary, fontSize: 12)),
              Text(
                '${acca.confidenceScore.toStringAsFixed(0)}%',
                style: const TextStyle(color: AppTheme.textPrimary, fontSize: 18, fontWeight: FontWeight.bold),
              ),
            ],
          ),
          StatusBadge(status: acca.status.value),
        ],
      ),
    );
  }
}
