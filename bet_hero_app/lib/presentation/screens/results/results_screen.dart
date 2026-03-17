import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:fl_chart/fl_chart.dart';
import '../../../core/theme.dart';
import '../../providers/results_provider.dart';
import '../../widgets/stats/calendar_heatmap.dart';
import '../../widgets/stats/history_card.dart';
import '../../widgets/stats/performance_stat_card.dart';


class ResultsScreen extends ConsumerWidget {
  const ResultsScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return DefaultTabController(
      length: 2,
      child: Scaffold(
        appBar: AppBar(
          title: const Text('RESULTS'),
          bottom: const TabBar(
            tabs: [
              Tab(text: 'HISTORY'),
              Tab(text: 'PERFORMANCE'),
            ],
            indicatorColor: AppTheme.primaryGold,
            labelColor: AppTheme.primaryGold,
            unselectedLabelColor: AppTheme.textSecondary,
          ),
        ),
        body: const TabBarView(
          children: [
            _HistoryTab(),
            _PerformanceTab(),
          ],
        ),
      ),
    );
  }
}

class _HistoryTab extends ConsumerWidget {
  const _HistoryTab();

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final historyAsync = ref.watch(accumulatorHistoryProvider);

    return RefreshIndicator(
      onRefresh: () async => ref.invalidate(accumulatorHistoryProvider),
      child: historyAsync.when(
        data: (history) {
          if (history.isEmpty) return const Center(child: Text('No history found'));
          return ListView.builder(
            padding: const EdgeInsets.all(16),
            itemCount: history.length,
            itemBuilder: (context, index) {
              final item = history[index];
              return AccumulatorHistoryCard(
                id: item.id,
                index: index,
                date: item.createdAt.toString().substring(0, 10),
                type: item.type.value,
                odds: item.totalOdds,
                status: item.status.value,
                onTap: () {},
              );
            },
          );
        },
        loading: () => const Center(child: CircularProgressIndicator()),
        error: (e, _) => Center(child: Text('Error: $e')),
      ),
    );
  }
}

class _PerformanceTab extends ConsumerWidget {
  const _PerformanceTab();

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final performanceAsync = ref.watch(performanceStatsProvider);

    return performanceAsync.when(
      data: (stats) => ListView(
        padding: const EdgeInsets.all(16),
        children: [
          Row(
            children: [
              Expanded(child: PerformanceStatCard(label: 'Total Bets', value: '${stats.totalPredictions}', trend: '${stats.pending} pending')),
              const SizedBox(width: 12),
              Expanded(child: PerformanceStatCard(label: 'Win Rate', value: '${stats.winRate.toStringAsFixed(1)}%', trend: '${stats.won} wins / ${stats.lost} losses', isPositiveTrend: true)),
            ],
          ),
          const SizedBox(height: 32),
          const Text('Win Rate by Acca Type', style: TextStyle(fontWeight: FontWeight.bold, fontSize: 14)),
          const SizedBox(height: 16),
          SizedBox(height: 200, child: _buildBarChart(stats.byType)),
          const SizedBox(height: 32),
          const CalendarHeatmap(results: {
            1: 'won', 2: 'won', 3: 'lost', 4: 'won', 5: 'won',
            6: 'lost', 7: 'lost', 8: 'won', 10: 'won', 12: 'won',
          }),
          const SizedBox(height: 32),
          const Text('Model Performance Breakdown', style: TextStyle(fontWeight: FontWeight.bold, fontSize: 14)),
          const SizedBox(height: 12),
          if (stats.models.isEmpty)
            const Text('No model data available', style: TextStyle(color: AppTheme.textSecondary, fontSize: 12))
          else
            ...stats.models.map((m) => _sportRankItem(
              m.modelName, 
              '${m.accuracy.toStringAsFixed(1)}% Acc', 
              '${m.totalBets} bets'
            )),
          const SizedBox(height: 100),
        ],
      ),
      loading: () => const Center(child: CircularProgressIndicator()),
      error: (e, _) => const Center(child: Text('Performance data unavailable')),
    );
  }

  Widget _buildBarChart(Map<String, dynamic> byType) {
    double getVal(String key) {
      final val = byType[key];
      if (val is Map) return (val['win_rate'] as num? ?? 0.0).toDouble();
      return (val as num? ?? 0.0).toDouble();
    }

    return BarChart(
      BarChartData(
        alignment: BarChartAlignment.spaceAround,
        maxY: 100,
        barTouchData: BarTouchData(enabled: true),
        titlesData: FlTitlesData(
          show: true,
          bottomTitles: AxisTitles(
            sideTitles: SideTitles(
              showTitles: true,
              getTitlesWidget: (val, _) {
                switch (val.toInt()) {
                  case 0: return const Text('10 Odds', style: TextStyle(fontSize: 10, color: Colors.white70));
                  case 1: return const Text('5 Odds', style: TextStyle(fontSize: 10, color: Colors.white70));
                  case 2: return const Text('3 Odds', style: TextStyle(fontSize: 10, color: Colors.white70));
                  default: return const Text('');
                }
              },
            ),
          ),
          leftTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
          topTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
          rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
        ),
        gridData: const FlGridData(show: false),
        borderData: FlBorderData(show: false),
        barGroups: [
          _makeBar(0, getVal('10odds'), AppTheme.primaryGold),
          _makeBar(1, getVal('5odds'), AppTheme.successGreen),
          _makeBar(2, getVal('3odds'), AppTheme.successGreen),
        ],
      ),
    );
  }

  BarChartGroupData _makeBar(int x, double y, Color color) {
    return BarChartGroupData(
      x: x,
      barRods: [
        BarChartRodData(
          toY: y,
          color: color,
          width: 24,
          borderRadius: BorderRadius.circular(4),
          backDrawRodData: BackgroundBarChartRodData(show: true, toY: 100, color: Colors.white.withOpacity(0.05)),
        ),
      ],
    );
  }

  Widget _sportRankItem(String sport, String wr, String roi) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 8),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(sport, style: const TextStyle(fontWeight: FontWeight.w600)),
          Row(
            children: [
              Text(wr, style: const TextStyle(color: AppTheme.textSecondary, fontSize: 12)),
              const SizedBox(width: 12),
              Text(roi, style: const TextStyle(color: AppTheme.successGreen, fontSize: 12, fontWeight: FontWeight.bold)),
            ],
          ),
        ],
      ),
    );
  }
}
