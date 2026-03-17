import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:go_router/go_router.dart';
import 'package:shimmer/shimmer.dart';
import 'package:intl/intl.dart';
import '../../../core/theme.dart';
import '../../../data/models/fixture_model.dart';

import '../../providers/fixture_provider.dart';
import '../../widgets/fixtures/fixture_card.dart';
import '../../widgets/fixtures/league_header.dart';


class FixturesScreen extends ConsumerStatefulWidget {
  const FixturesScreen({super.key});

  @override
  ConsumerState<FixturesScreen> createState() => _FixturesScreenState();
}

class _FixturesScreenState extends ConsumerState<FixturesScreen> {
  DateTime _selectedDate = DateTime.now();

  @override
  Widget build(BuildContext context) {
    final fixturesAsync = ref.watch(fixturesBySportProvider(ref.watch(selectedSportProvider)));
    final selectedSport = ref.watch(selectedSportProvider);
    final selectedDate = ref.watch(selectedDateProvider);

    return Scaffold(
      appBar: AppBar(
        title: const Text('FIXTURES'),
        actions: [
          IconButton(
            onPressed: () => ref.invalidate(fixturesBySportProvider(selectedSport)),
            icon: const Icon(Icons.refresh),
          ),
        ],
      ),
      body: Column(
        children: [
          _buildDateSelector(selectedDate),
          _buildSportFilters(),
          Expanded(
            child: RefreshIndicator(
              onRefresh: () async => ref.invalidate(fixturesBySportProvider(selectedSport)),
              child: fixturesAsync.when(
                data: (fixtures) => _buildFixtureList(fixtures),
                loading: () => _buildShimmerLoading(),
                error: (e, _) => Center(
                  child: Column(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      Icon(Icons.sports_soccer, size: 64, color: Colors.grey[700]),
                      const SizedBox(height: 16),
                      const Text(
                        "No fixtures available yet",
                        style: TextStyle(color: Colors.grey),
                      ),
                      const SizedBox(height: 8),
                      Text(
                        "Live data coming soon",
                        style: TextStyle(color: Colors.grey[600], fontSize: 12),
                      ),
                      const SizedBox(height: 24),
                      OutlinedButton(
                        onPressed: () => ref.refresh(fixturesBySportProvider(selectedSport)),
                        child: const Text("Retry"),
                      )
                    ],
                  ),
                ),
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildDateSelector(DateTime selectedDate) {
    return Container(
      height: 80,
      padding: const EdgeInsets.symmetric(vertical: 8),
      color: AppTheme.cardBackground,
      child: ListView.builder(
        scrollDirection: Axis.horizontal,
        itemCount: 7, // Future 7 days only
        padding: const EdgeInsets.symmetric(horizontal: 12),
        itemBuilder: (context, index) {
          final date = DateTime.now().add(Duration(days: index));
          final isSelected = DateFormat('yyyy-MM-dd').format(date) == DateFormat('yyyy-MM-dd').format(selectedDate);

          return GestureDetector(
            onTap: () => ref.read(selectedDateProvider.notifier).state = date,
            child: Container(
              width: 50,
              margin: const EdgeInsets.symmetric(horizontal: 4),
              decoration: BoxDecoration(
                color: isSelected ? AppTheme.primaryGold : Colors.transparent,
                borderRadius: BorderRadius.circular(8),
              ),
              child: Column(
                mainAxisAlignment: MainAxisAlignment.center,
                children: [
                  Text(
                    DateFormat('E').format(date).toUpperCase(),
                    style: TextStyle(
                      color: isSelected ? Colors.black : AppTheme.textSecondary,
                      fontSize: 10,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(
                    DateFormat('d').format(date),
                    style: TextStyle(
                      color: isSelected ? Colors.black : AppTheme.textPrimary,
                      fontSize: 16,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                ],
              ),
            ),
          );
        },
      ),
    );
  }

  Widget _buildSportFilters() {
    final sports = ['All', 'Football', 'Basketball', 'Tennis', 'NFL', 'Cricket', 'NHL', 'MLB'];
    final selected = ref.watch(selectedSportProvider);

    return Container(
      height: 50,
      padding: const EdgeInsets.symmetric(vertical: 8),
      child: ListView.builder(
        scrollDirection: Axis.horizontal,
        padding: const EdgeInsets.symmetric(horizontal: 16),
        itemCount: sports.length,
        itemBuilder: (context, index) {
          final sport = sports[index];
          final isSelected = selected.toLowerCase() == sport.toLowerCase();
          return Padding(
            padding: const EdgeInsets.only(right: 8),
            child: ChoiceChip(
              label: Text(sport),
              selected: isSelected,
              onSelected: (val) => ref.read(selectedSportProvider.notifier).state = sport.toLowerCase(),
              selectedColor: AppTheme.primaryGold,
              backgroundColor: AppTheme.cardBackground,
              labelStyle: TextStyle(color: isSelected ? Colors.black : AppTheme.textPrimary, fontSize: 12),
            ),
          );
        },
      ),
    );
  }

  Widget _buildFixtureList(List<FixtureModel> fixtures) {
    final selectedSport = ref.watch(selectedSportProvider);
    // Filter by selected date (Note: Ideally API does this, but keeping it robust)
    final filtered = fixtures.where((f) => DateFormat('yyy-MM-dd').format(f.matchDate) == DateFormat('yyy-MM-dd').format(ref.read(selectedDateProvider))).toList();

    if (filtered.isEmpty) {
      return Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            Icon(Icons.sports_soccer, size: 64, color: Colors.grey[700]),
            const SizedBox(height: 16),
            const Text(
              "No fixtures available yet",
              style: TextStyle(color: Colors.grey),
            ),
            const SizedBox(height: 8),
            Text(
              "Live data coming soon",
              style: TextStyle(color: Colors.grey[600], fontSize: 12),
            ),
            const SizedBox(height: 24),
            OutlinedButton(
              onPressed: () => ref.refresh(fixturesBySportProvider(selectedSport)),
              child: const Text("Retry"),
            )
          ],
        ),
      );
    }

    // Group by league
    Map<String, List<FixtureModel>> grouped = {};
    for (var f in filtered) {
      final key = f.league;
      if (!grouped.containsKey(key)) grouped[key] = [];
      grouped[key]!.add(f);
    }

    return ListView.builder(
      itemCount: grouped.length,
      itemBuilder: (context, index) {
        final league = grouped.keys.elementAt(index);
        final leagueFixtures = grouped[league]!;
        return Column(
          children: [
            LeagueGroupHeader(leagueName: league),
            ...leagueFixtures.map((f) => FixtureCard(
              fixture: f,
              onTap: () => context.push('/fixture/${f.id}'),
            )),
          ],
        );
      },
    );
  }

  Widget _buildShimmerLoading() {
    return Shimmer.fromColors(
      baseColor: Colors.white12,
      highlightColor: Colors.white24,
      child: ListView.builder(
        itemCount: 5,
        itemBuilder: (_, __) => Padding(
          padding: const EdgeInsets.all(16.0),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Container(width: 150, height: 20, color: Colors.white),
              const SizedBox(height: 16),
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  Container(width: 100, height: 40, color: Colors.white),
                  Container(width: 100, height: 40, color: Colors.white),
                ],
              ),
            ],
          ),
        ),
      ),
    );
  }
}
