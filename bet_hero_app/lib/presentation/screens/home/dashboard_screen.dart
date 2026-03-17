import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:go_router/go_router.dart';
import '../../../core/theme.dart';
import '../../../data/models/accumulator_model.dart';
import '../../providers/accumulator_provider.dart';
import '../../providers/fixture_provider.dart';
import '../../widgets/accumulator/accumulator_summary_card.dart';
import '../../widgets/accumulator/leg_card.dart';

class DashboardScreen extends ConsumerWidget {
  const DashboardScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final accasAsync = ref.watch(todayAccumulatorsProvider);
    final fixturesAsync = ref.watch(upcomingFixturesProvider);
    final selectedSport = ref.watch(selectedSportProvider);

    return Scaffold(
      body: CustomScrollView(
        slivers: [
          _buildAppBar(context),
          SliverToBoxAdapter(
            child: Padding(
              padding: const EdgeInsets.all(16.0),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  _buildSectionHeader('Today\'s Summaries'),
                  const SizedBox(height: 12),
                  _buildSummaryCards(accasAsync, context),
                  const SizedBox(height: 32),
                  _buildSectionHeader('Featured Accumulator'),
                  const SizedBox(height: 12),
                  _buildFeaturedCard(accasAsync, context),
                  const SizedBox(height: 32),
                  _buildSectionHeader('Upcoming Fixtures'),
                  const SizedBox(height: 12),
                  _buildSportFilters(ref),
                  const SizedBox(height: 12),
                  _buildFixturesPreview(fixturesAsync, selectedSport),
                  const SizedBox(height: 100), // Navigation spacing
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildAppBar(BuildContext context) {
    return SliverAppBar(
      floating: true,
      title: Column(
        children: [
          const Text('BET HERO', style: TextStyle(letterSpacing: 2, fontWeight: FontWeight.bold)),
          Text(
            'TODAY\'S PREDICTIONS',
            style: TextStyle(color: AppTheme.primaryGold, fontSize: 10, letterSpacing: 1.5),
          ),
        ],
      ),
      actions: [
        IconButton(
          onPressed: () => context.push('/notifications'),
          icon: const Icon(Icons.notifications_none),
        ),
        IconButton(
          onPressed: () => context.push('/settings'),
          icon: const Icon(Icons.person_outline),
        ),
      ],
    );
  }

  Widget _buildSectionHeader(String title) {
    return Text(
      title,
      style: const TextStyle(
        fontSize: 18,
        fontWeight: FontWeight.bold,
        letterSpacing: 0.5,
      ),
    );
  }

  Widget _buildSummaryCards(AsyncValue<List<AccumulatorModel>> accasAsync, BuildContext context) {
    return accasAsync.when(
      data: (accas) {
        return SizedBox(
          height: 100,
          child: Row(
            children: [
              _summaryCard(accas, AccaType.ten, context, 0),
              const SizedBox(width: 12),
              _summaryCard(accas, AccaType.five, context, 1),
              const SizedBox(width: 12),
              _summaryCard(accas, AccaType.three, context, 2),
            ],
          ),
        );
      },
      loading: () => const SizedBox(height: 100, child: Center(child: CircularProgressIndicator())),
      error: (e, _) => const Text('Failed to load accumulators'),
    );
  }

  Widget _summaryCard(List<AccumulatorModel> accas, AccaType type, BuildContext context, int index) {
    final acca = accas.firstWhere((a) => a.type == type, orElse: () => _emptyAcca(type));
    return Expanded(
      child: AccumulatorSummaryCard(
        accumulator: acca,
        index: index,
        onTap: () => context.push('/accumulator/${type.value}?index=$index'),
      ),
    );
  }

  Widget _buildFeaturedCard(AsyncValue<List<AccumulatorModel>> accasAsync, BuildContext context) {
    return accasAsync.when(
      data: (accas) {
        final featured = accas.isEmpty 
            ? _emptyAcca(AccaType.ten)
            : accas.firstWhere((a) => a.type == AccaType.ten, orElse: () => accas.first);
        return Container(
          decoration: BoxDecoration(
            color: AppTheme.cardBackground,
            borderRadius: BorderRadius.circular(16),
            border: Border(
              bottom: BorderSide(
                color: Colors.white.withValues(alpha: 0.05),
                width: 1.0,
              ),
            ),
          ),
          child: Column(
            children: [
              Padding(
                padding: const EdgeInsets.all(16.0),
                child: Row(
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  children: [
                    const Text('10 ODDS COMBO', style: TextStyle(fontWeight: FontWeight.bold)),
                    Text('${featured.totalOdds.toStringAsFixed(2)} ODDS', style: const TextStyle(color: AppTheme.primaryGold, fontWeight: FontWeight.bold)),
                  ],
                ),
              ),
              ...featured.legs.take(3).map((leg) => Padding(
                padding: const EdgeInsets.symmetric(horizontal: 16),
                child: LegCard(leg: leg),
              )),
              Padding(
                padding: const EdgeInsets.all(16.0),
                child: SizedBox(
                  width: double.infinity,
                  child: ElevatedButton(
                    onPressed: () => context.push('/accumulator/${featured.type.value}?index=10'),
                    child: const Text('View Full Analysis'),
                  ),
                ),
              ),
            ],
          ),
        );
      },
      loading: () => const Center(child: CircularProgressIndicator()),
      error: (e, _) => const SizedBox.shrink(),
    );
  }

  Widget _buildSportFilters(WidgetRef ref) {
    final sports = ['All', 'Football', 'Basketball', 'Tennis', 'NFL', 'Cricket', 'NHL', 'MLB'];
    final selected = ref.watch(selectedSportProvider);

    return SizedBox(
      height: 40,
      child: ListView.builder(
        scrollDirection: Axis.horizontal,
        itemCount: sports.length,
        itemBuilder: (context, index) {
          final sport = sports[index];
          final isSelected = selected.toLowerCase() == sport.toLowerCase();
          return Padding(
            padding: const EdgeInsets.only(right: 8),
            child: FilterChip(
              label: Text(sport),
              selected: isSelected,
              onSelected: (val) => ref.read(selectedSportProvider.notifier).state = sport.toLowerCase(),
              selectedColor: AppTheme.primaryGold,
              labelStyle: TextStyle(color: isSelected ? Colors.black : Colors.white),
            ),
          );
        },
      ),
    );
  }

  Widget _buildFixturesPreview(AsyncValue<List> fixturesAsync, String selectedSport) {
    return fixturesAsync.when(
      data: (fixtures) {
        final filtered = selectedSport == 'all' 
          ? fixtures 
          : fixtures.where((f) => f.sport.toLowerCase() == selectedSport).toList();

        return SizedBox(
          height: 120,
          child: ListView.builder(
            scrollDirection: Axis.horizontal,
            itemCount: filtered.length,
            itemBuilder: (context, index) {
              final fixture = filtered[index];
              return Container(
                width: 200,
                margin: const EdgeInsets.only(right: 12),
                padding: const EdgeInsets.all(12),
                decoration: BoxDecoration(
                  color: AppTheme.cardBackground,
                  borderRadius: BorderRadius.circular(12),
                ),
                child: Column(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: [
                    Text(fixture.homeTeam, maxLines: 1, overflow: TextOverflow.ellipsis, style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 12)),
                    const Text('vs', style: TextStyle(color: AppTheme.textSecondary, fontSize: 10)),
                    Text(fixture.awayTeam, maxLines: 1, overflow: TextOverflow.ellipsis, style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 12)),
                    const SizedBox(height: 8),
                    Text(fixture.matchDate.toString().substring(11, 16), style: const TextStyle(color: AppTheme.primaryGold, fontSize: 10)),
                  ],
                ),
              );
            },
          ),
        );
      },
      loading: () => const Center(child: CircularProgressIndicator()),
      error: (e, _) => const Text('Fixtures unavailable'),
    );
  }

  AccumulatorModel _emptyAcca(AccaType type) {
    return AccumulatorModel(
      id: '0',
      type: type,
      totalOdds: 0.0,
      status: AccaStatus.pending,
      confidenceScore: 0.0,
      createdAt: DateTime.now(),
      legs: [],
    );
  }
}
