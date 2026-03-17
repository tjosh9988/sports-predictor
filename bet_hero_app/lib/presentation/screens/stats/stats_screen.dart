import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:fl_chart/fl_chart.dart';
import '../../../core/theme.dart';


class StatsScreen extends ConsumerWidget {
  const StatsScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('MODEL STATS'),
      ),
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: [
          _buildCard(
            title: 'Accuracy Over Time',
            child: SizedBox(height: 200, child: _buildAccuracyLineChart()),
          ),
          const SizedBox(height: 20),
          _buildCard(
            title: 'Confidence Calibration',
            subtitle: 'Actual Win Rate vs Model Confidence',
            child: SizedBox(height: 200, child: _buildCalibrationChart()),
          ),
          const SizedBox(height: 20),
          Row(
            children: [
              Expanded(
                child: _buildCard(
                  title: 'Success Rate',
                  child: SizedBox(height: 150, child: _buildDonutChart()),
                ),
              ),
              const SizedBox(width: 16),
              Expanded(
                child: _buildCard(
                  title: 'Top Markets',
                  child: Column(
                    children: [
                      _marketStat('BTTS', '74%', true),
                      _marketStat('Over 2.5', '68%', true),
                      _marketStat('Home Win', '61%', false),
                    ],
                  ),
                ),
              ),
            ],
          ),
          const SizedBox(height: 20),
          _buildCard(
            title: 'Monthly ROI Trend',
            child: SizedBox(height: 200, child: _buildROITrendChart()),
          ),
          const SizedBox(height: 100),
        ],
      ),
    );
  }

  Widget _buildCard({required String title, String? subtitle, required Widget child}) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: AppTheme.cardBackground,
        borderRadius: BorderRadius.circular(16),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(title, style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 16)),
          if (subtitle != null) ...[
            const SizedBox(height: 4),
            Text(subtitle, style: const TextStyle(color: AppTheme.textSecondary, fontSize: 10)),
          ],
          const SizedBox(height: 20),
          child,
        ],
      ),
    );
  }

  Widget _buildAccuracyLineChart() {
    return LineChart(
      LineChartData(
        gridData: const FlGridData(show: false),
        titlesData: _chartTitles(),
        borderData: FlBorderData(show: false),
        lineBarsData: [
          LineChartBarData(
            spots: [
              const FlSpot(0, 60), const FlSpot(1, 45), const FlSpot(2, 65),
              const FlSpot(3, 70), const FlSpot(4, 68), const FlSpot(5, 75),
            ],
            isCurved: true,
            color: AppTheme.primaryGold,
            barWidth: 3,
            dotData: const FlDotData(show: false),
            belowBarData: BarAreaData(
              show: true,
              color: AppTheme.primaryGold.withOpacity(0.1),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildCalibrationChart() {
    return LineChart(
      LineChartData(
        gridData: FlGridData(
          show: true,
          drawVerticalLine: true,
          getDrawingHorizontalLine: (val) => FlLine(color: Colors.white10, strokeWidth: 1),
          getDrawingVerticalLine: (val) => FlLine(color: Colors.white10, strokeWidth: 1),
        ),
        titlesData: _chartTitles(),
        borderData: FlBorderData(show: false),
        lineBarsData: [
          // Perfect calibration line
          LineChartBarData(
            spots: [const FlSpot(0, 0), const FlSpot(5, 50), const FlSpot(10, 100)],
            isCurved: false,
            color: Colors.white24,
            dashArray: [5, 5],
            dotData: const FlDotData(show: false),
          ),
          // Actual model performance
          LineChartBarData(
            spots: [
              const FlSpot(0, 5), const FlSpot(2, 22), const FlSpot(4, 38),
              const FlSpot(6, 65), const FlSpot(8, 82), const FlSpot(10, 94),
            ],
            isCurved: true,
            color: AppTheme.successGreen,
            barWidth: 3,
            dotData: const FlDotData(show: true),
          ),
        ],
      ),
    );
  }

  Widget _buildDonutChart() {
    return PieChart(
      PieChartData(
        sectionsSpace: 0,
        centerSpaceRadius: 40,
        sections: [
          PieChartSectionData(color: AppTheme.successGreen, value: 65, title: '65%', radius: 20, titleStyle: const TextStyle(fontSize: 10, fontWeight: FontWeight.bold)),
          PieChartSectionData(color: AppTheme.dangerRed, value: 35, title: '35%', radius: 20, titleStyle: const TextStyle(fontSize: 10, fontWeight: FontWeight.bold)),
        ],
      ),
    );
  }

  Widget _buildROITrendChart() {
    return BarChart(
      BarChartData(
        alignment: BarChartAlignment.spaceEvenly,
        maxY: 40,
        titlesData: _chartTitles(),
        gridData: const FlGridData(show: false),
        borderData: FlBorderData(show: false),
        barGroups: [
          _bar(0, 15), _bar(1, 28), _bar(2, 22), _bar(3, 35), _bar(4, 31),
        ],
      ),
    );
  }

  BarChartGroupData _bar(int x, double y) {
    return BarChartGroupData(
      x: x,
      barRods: [
        BarChartRodData(
          toY: y,
          color: AppTheme.primaryGold,
          width: 16,
          borderRadius: BorderRadius.circular(4),
        ),
      ],
    );
  }

  FlTitlesData _chartTitles() {
    return const FlTitlesData(
      show: true,
      leftTitles: AxisTitles(sideTitles: SideTitles(showTitles: false)),
      topTitles: AxisTitles(sideTitles: SideTitles(showTitles: false)),
      rightTitles: AxisTitles(sideTitles: SideTitles(showTitles: false)),
    );
  }

  Widget _marketStat(String label, String val, bool isGood) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 8),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(label, style: const TextStyle(color: AppTheme.textSecondary, fontSize: 12)),
          Text(val, style: TextStyle(color: isGood ? AppTheme.successGreen : AppTheme.textPrimary, fontWeight: FontWeight.bold)),
        ],
      ),
    );
  }
}
