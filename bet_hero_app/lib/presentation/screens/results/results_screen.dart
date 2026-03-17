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
    final historyAsync = ref.watch(predictionHistoryProvider);

    return RefreshIndicator(
      onRefresh: () async => ref.invalidate(predictionHistoryProvider),
      child: historyAsync.when(
        data: (history) {
          if (history.isEmpty) return const Center(child: Text('No history found'));
          return ListView.builder(
            padding: const EdgeInsets.all(16),
            itemCount: history.length,
            itemBuilder: (context, index) {
              final item = history[index];
              return AccumulatorHistoryCard(
                date: item.matchDate.toString().substring(0, 10),
                type: item.market,
                odds: item.odds,
                status: item.result ?? 'pending',
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
              Expanded(child: PerformanceStatCard(label: 'Total ROI', value: '+${stats.roi.toStringAsFixed(1)}%', trend: '12% this month')),
              const SizedBox(width: 12),
              Expanded(child: PerformanceStatCard(label: 'Win Rate', value: '${stats.winRate.toStringAsFixed(1)}%', trend: '4% increase', isPositiveTrend: true)),
            ],
          ),
          const SizedBox(height: 32),
          const Text('Win Rate by Acca Type', style: TextStyle(fontWeight: FontWeight.bold, fontSize: 14)),
          const SizedBox(height: 16),
          SizedBox(height: 200, child: _buildBarChart()),
          const SizedBox(height: 32),
          const CalendarHeatmap(results: {
            1: 'won', 2: 'won', 3: 'lost', 4: 'won', 5: 'won',
            6: 'lost', 7: 'lost', 8: 'won', 10: 'won', 12: 'won',
          }),
          const SizedBox(height: 32),
          const Text('Top Performing Sports', style: TextStyle(fontWeight: FontWeight.bold, fontSize: 14)),
          const SizedBox(height: 12),
          _sportRankItem('Football', '68% WR', '+24% ROI'),
          _sportRankItem('Basketball', '62% WR', '+18% ROI'),
          _sportRankItem('Tennis', '58% WR', '+12% ROI'),
          const SizedBox(height: 100),
        ],
      ),
      loading: () => const Center(child: CircularProgressIndicator()),
      error: (e, _) => const Center(child: Text('Performance data unavailable')),
    );
  }

  Widget _buildBarChart() {
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
          _makeBar(0, 48, AppTheme.primaryGold),
          _makeBar(1, 62, AppTheme.successGreen),
          _makeBar(2, 74, AppTheme.successGreen),
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
