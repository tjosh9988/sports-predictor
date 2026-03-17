import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:flutter_riverpod/legacy.dart';
import '../../core/di.dart';
import '../../data/models/fixture_model.dart';

// Provider for matches filtered by sport
final fixturesBySportProvider = FutureProvider.family<List<FixtureModel>, String>((ref, sport) async {
  ref.keepAlive();
  final repo = ref.watch(fixtureRepositoryProvider);
  final date = ref.watch(selectedDateProvider);
  try {
    return await repo.getFixturesBySport(sport, date);
  } catch (e) {
    return [];
  }
});

// State provider for current selected sport
final selectedSportProvider = StateProvider<String>((ref) => 'football');

// State provider for current selected match date
final selectedDateProvider = StateProvider<DateTime>((ref) => DateTime.now());

// Provider for specific fixture details
final fixtureDetailProvider = FutureProvider.family<FixtureModel?, String>((ref, id) async {
  final repo = ref.watch(fixtureRepositoryProvider);
  try {
    return await repo.getFixtureById(id);
  } catch (e) {
    return null;
  }
});

// Upcoming fixtures across all sports
final upcomingFixturesProvider = FutureProvider<List<FixtureModel>>((ref) async {
  final repo = ref.watch(fixtureRepositoryProvider);
  try {
    return await repo.getUpcomingFixtures();
  } catch (e) {
    return [];
  }
});
