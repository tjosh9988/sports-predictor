import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:intl/intl.dart';
import '../../../core/theme.dart';

import '../../../data/models/fixture_model.dart';
import '../../../data/models/prediction_model.dart';
import '../../providers/fixture_provider.dart';
import '../../widgets/fixtures/form_badge.dart';
import '../../widgets/fixtures/stat_comparison_row.dart';


class FixtureDetailScreen extends ConsumerWidget {
  final String id;

  const FixtureDetailScreen({super.key, required this.id});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final fixtureAsync = ref.watch(fixtureDetailProvider(id));

    return DefaultTabController(
      length: 4,
      child: Scaffold(
        appBar: AppBar(
          title: const Text('MATCH CENTER'),
          bottom: const TabBar(
            isScrollable: true,
            tabs: [
              Tab(text: 'OVERVIEW'),
              Tab(text: 'PREDICTIONS'),
              Tab(text: 'H2H'),
              Tab(text: 'STATS'),
            ],
            indicatorColor: AppTheme.primaryGold,
            labelColor: AppTheme.primaryGold,
            unselectedLabelColor: AppTheme.textSecondary,
          ),
        ),
        body: fixtureAsync.when(
          data: (fixture) {
            if (fixture == null) return const Center(child: Text('Fixture not found'));
            return TabBarView(
              children: [
                _buildOverviewTab(fixture),
                _buildPredictionsTab(fixture),
                _buildH2HTab(fixture),
                _buildStatsTab(fixture),
              ],
            );
          },
          loading: () => const Center(child: CircularProgressIndicator()),
          error: (e, _) => Center(child: Text('Error: $e')),
        ),
      ),
    );
  }

  Widget _buildOverviewTab(dynamic fixture) {
    return ListView(
      padding: const EdgeInsets.all(16),
      children: [
        _buildMatchHeader(fixture),
        const SizedBox(height: 32),
        const Text('BOOKMAKER ODDS', style: TextStyle(fontWeight: FontWeight.bold)),
        const SizedBox(height: 16),
        _buildOddsSection(fixture),
        const SizedBox(height: 32),
        const Text('TEAM FORM (LAST 5)', style: TextStyle(fontWeight: FontWeight.bold)),
        const SizedBox(height: 16),
        _buildFormRow('Home Team', ['W', 'D', 'W', 'W', 'L']),
        _buildFormRow('Away Team', ['L', 'L', 'D', 'W', 'L']),
      ],
    );
  }

  Widget _buildMatchHeader(dynamic fixture) {
    return Hero(
      tag: 'fixture_${fixture.id}',
      child: Container(
        padding: const EdgeInsets.all(24),
        decoration: BoxDecoration(
          color: AppTheme.cardBackground,
          borderRadius: BorderRadius.circular(16),
        ),
        child: Material(
          color: Colors.transparent,
          child: Column(
            children: [
              Text(fixture.league ?? 'LEAGUE', style: const TextStyle(color: AppTheme.primaryGold, fontSize: 10, letterSpacing: 2)),
              const SizedBox(height: 16),
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceEvenly,
                children: [
                  Expanded(child: Text(fixture.homeTeam, textAlign: TextAlign.center, style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 18, color: AppTheme.textPrimary))),
                  Container(
                    padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 4),
                    decoration: BoxDecoration(color: AppTheme.secondaryBackground, borderRadius: BorderRadius.circular(4)),
                    child: Text(
                      fixture.homeScore != null ? '${fixture.homeScore} - ${fixture.awayScore}' : DateFormat('HH:mm').format(fixture.matchDate.toLocal()),
                      style: const TextStyle(fontWeight: FontWeight.bold, color: AppTheme.textPrimary),
                    ),
                  ),
                  Expanded(child: Text(fixture.awayTeam, textAlign: TextAlign.center, style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 18, color: AppTheme.textPrimary))),
                ],
              ),
              const SizedBox(height: 16),
              Text(DateFormat('EEEE, d MMMM yyyy').format(fixture.matchDate.toLocal()), style: const TextStyle(color: AppTheme.textSecondary, fontSize: 12)),
              if (fixture.venue != null)
                Text(fixture.venue!, style: const TextStyle(color: AppTheme.textSecondary, fontSize: 12)),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildOddsSection(dynamic fixture) {
    return Row(
      children: [
        Expanded(child: _oddsTile('Bet365', fixture.homeOdds ?? 0, 'Home')),
        const SizedBox(width: 8),
        Expanded(child: _oddsTile('Betway', fixture.drawOdds ?? 0, 'Draw')),
        const SizedBox(width: 8),
        Expanded(child: _oddsTile('1xBet', fixture.awayOdds ?? 0, 'Away')),
      ],
    );
  }

  Widget _oddsTile(String bookie, double val, String label) {
    return Container(
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(color: AppTheme.cardBackground, borderRadius: BorderRadius.circular(8)),
      child: Column(
        children: [
          Text(bookie, style: const TextStyle(color: AppTheme.textSecondary, fontSize: 10)),
          const SizedBox(height: 4),
          Text(val.toStringAsFixed(2), style: const TextStyle(color: AppTheme.primaryGold, fontWeight: FontWeight.bold, fontSize: 16)),
          Text(label, style: const TextStyle(color: AppTheme.textSecondary, fontSize: 10)),
        ],
      ),
    );
  }

  Widget _buildFormRow(String team, List<String> form) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 8),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(team, style: const TextStyle(color: AppTheme.textPrimary)),
          Row(
            children: form.map((res) => Padding(
              padding: const EdgeInsets.only(left: 4),
              child: FormBadge(result: res),
            )).toList(),
          ),
        ],
      ),
    );
  }

  Widget _buildPredictionsTab(FixtureModel fixture) {
    final predictions = fixture.predictions ?? [];
    
    return ListView(
      padding: const EdgeInsets.all(16),
      children: [
        const Text('AI MARKET BREAKDOWN', style: TextStyle(fontWeight: FontWeight.bold)),
        const SizedBox(height: 16),
        if (predictions.isEmpty)
          const Center(
            child: Padding(
              padding: EdgeInsets.symmetric(vertical: 32),
              child: Text(
                'No detailed predictions available yet.',
                style: TextStyle(color: AppTheme.textSecondary),
              ),
            ),
          )
        else
          ...predictions.map((p) => _buildPredictionCard(p)),
        const SizedBox(height: 32),
        if (fixture.predictionId != null)
          Container(
            padding: const EdgeInsets.all(16),
            decoration: BoxDecoration(color: AppTheme.primaryGold.withOpacity(0.1), borderRadius: BorderRadius.circular(12)),
            child: const Row(
              children: [
                Icon(Icons.bolt, color: AppTheme.primaryGold),
                SizedBox(width: 12),
                Expanded(child: Text('This match is part of today\'s 10-Odds Accumulator!', style: TextStyle(color: AppTheme.primaryGold, fontWeight: FontWeight.bold))),
              ],
            ),
          ),
      ],
    );
  }

  Widget _buildPredictionCard(PredictionModel p) {
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
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Text(
                p.market,
                style: const TextStyle(color: AppTheme.primaryGold, fontWeight: FontWeight.bold, fontSize: 13),
              ),
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 2),
                decoration: BoxDecoration(
                  color: AppTheme.primaryGold.withOpacity(0.1),
                  borderRadius: BorderRadius.circular(4),
                ),
                child: Text(
                  p.odds.toStringAsFixed(2),
                  style: const TextStyle(color: AppTheme.primaryGold, fontWeight: FontWeight.bold, fontSize: 12),
                ),
              ),
            ],
          ),
          const SizedBox(height: 8),
          Text(
            p.predictedOutcome,
            style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 16, color: AppTheme.textPrimary),
          ),
          const SizedBox(height: 16),
          _buildDetailConfidenceBar(p.confidenceScore),
          const SizedBox(height: 12),
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Text(
                'Edge: +${p.edge.toStringAsFixed(1)}%',
                style: const TextStyle(color: AppTheme.successGreen, fontSize: 11, fontWeight: FontWeight.bold),
              ),
              Text(
                'Status: ${p.status.toUpperCase()}',
                style: TextStyle(
                  color: p.status == 'won' ? AppTheme.successGreen : (p.status == 'lost' ? AppTheme.dangerRed : AppTheme.textSecondary),
                  fontSize: 10,
                ),
              ),
            ],
          ),
          if (p.aiReasoning != null && p.aiReasoning!.isNotEmpty) ...[
            const SizedBox(height: 12),
            const Divider(color: Colors.white10),
            const SizedBox(height: 8),
            Text(
              p.aiReasoning!,
              style: const TextStyle(color: AppTheme.textSecondary, fontSize: 11, fontStyle: FontStyle.italic),
            ),
          ],
        ],
      ),
    );
  }

  Widget _buildDetailConfidenceBar(double confidence) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            const Text('Confidence', style: TextStyle(color: AppTheme.textSecondary, fontSize: 10)),
            Text('${confidence.toInt()}%', style: const TextStyle(color: AppTheme.textPrimary, fontSize: 10, fontWeight: FontWeight.bold)),
          ],
        ),
        const SizedBox(height: 4),
        ClipRRect(
          borderRadius: BorderRadius.circular(2),
          child: LinearProgressIndicator(
            value: confidence / 100,
            backgroundColor: Colors.white.withOpacity(0.05),
            valueColor: AlwaysStoppedAnimation<Color>(
              confidence >= 70 ? AppTheme.successGreen : (confidence >= 55 ? AppTheme.primaryGold : Colors.orange),
            ),
            minHeight: 4,
          ),
        ),
      ],
    );
  }

  Widget _buildH2HTab(dynamic fixture) {
    return ListView(
      padding: const EdgeInsets.all(16),
      children: [
        const Text('HEAD TO HEAD HISTORY', style: TextStyle(fontWeight: FontWeight.bold)),
        const SizedBox(height: 16),
        _h2hRecord(41, 23, 36), // W/D/L percentages for home team
        const SizedBox(height: 32),
        const Text('LAST 5 MEETINGS', style: TextStyle(fontWeight: FontWeight.bold)),
        const SizedBox(height: 16),
        _h2hMeeting('May 2023', '2 - 1', true),
        _h2hMeeting('Dec 2022', '0 - 0', null),
        _h2hMeeting('Oct 2021', '1 - 3', false),
        _h2hMeeting('Mar 2021', '2 - 2', null),
        _h2hMeeting('Jan 2020', '3 - 0', true),
      ],
    );
  }

  Widget _h2hRecord(int w, int d, int l) {
    return Row(
      children: [
        _recordPart('W', w, AppTheme.successGreen),
        _recordPart('D', d, Colors.orange),
        _recordPart('L', l, AppTheme.dangerRed),
      ],
    );
  }

  Widget _recordPart(String label, int val, Color color) {
    return Expanded(
      flex: val,
      child: Container(
        height: 30,
        alignment: Alignment.center,
        color: color.withOpacity(0.6),
        child: Text('$val% $label', style: const TextStyle(fontSize: 10, fontWeight: FontWeight.bold)),
      ),
    );
  }

  Widget _h2hMeeting(String date, String score, bool? homeWon) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 8),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(date, style: const TextStyle(color: AppTheme.textSecondary, fontSize: 12)),
          Text(score, style: const TextStyle(fontWeight: FontWeight.bold)),
          FormBadge(result: homeWon == null ? 'D' : (homeWon ? 'W' : 'L')),
        ],
      ),
    );
  }

  Widget _buildStatsTab(dynamic fixture) {
    return ListView(
      padding: const EdgeInsets.all(16),
      children: const [
        Text('SEASON COMPARISON', style: TextStyle(fontWeight: FontWeight.bold)),
        SizedBox(height: 16),
        StatComparisonRow(label: 'Avg Goals Scored', homeValue: '2.1', awayValue: '1.4', homeRatio: 0.6),
        StatComparisonRow(label: 'Avg Goals Conceded', homeValue: '0.9', awayValue: '1.8', homeRatio: 0.3),
        StatComparisonRow(label: 'Shots on Target', homeValue: '6.4', awayValue: '4.2', homeRatio: 0.65),
        StatComparisonRow(label: 'Possession %', homeValue: '54%', awayValue: '46%', homeRatio: 0.54),
        StatComparisonRow(label: 'Clean Sheets', homeValue: '12', awayValue: '5', homeRatio: 0.7),
      ],
    );
  }
}
