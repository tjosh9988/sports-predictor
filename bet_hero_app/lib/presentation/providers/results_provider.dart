import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../../core/di.dart';
import '../../data/models/prediction_model.dart';
import '../../data/models/performance_model.dart';

// Provider for overall performance statistics
final performanceStatsProvider = FutureProvider<PerformanceModel>((ref) async {
  final repo = ref.watch(resultsRepositoryProvider);
  try {
    return await repo.getPerformanceStats();
  } catch (e) {
    return PerformanceModel.empty();
  }
});

// Provider for accuracy breakdown by sport
final accuracyBySportProvider = FutureProvider.family<Map<String, double>, String>((ref, sport) async {
  final repo = ref.watch(resultsRepositoryProvider);
  return await repo.getAccuracyBySport(sport);
});

// AsyncNotifier for paginated prediction history
class PredictionHistoryNotifier extends AsyncNotifier<List<PredictionModel>> {
  int _currentPage = 1;
  bool _hasMore = true;

  @override
  Future<List<PredictionModel>> build() async {
    _currentPage = 1;
    _hasMore = true;
    return await ref.read(resultsRepositoryProvider).getPredictionHistory(page: 1);
  }

  Future<void> fetchNextPage() async {
    if (!_hasMore || state.isLoading) return;
    
    final currentList = state.value ?? [];
    _currentPage++;
    
    final nextPage = await ref.read(resultsRepositoryProvider).getPredictionHistory(page: _currentPage);
    if (nextPage.isEmpty) {
      _hasMore = false;
    } else {
      state = AsyncData([...currentList, ...nextPage]);
    }
  }
}

final predictionHistoryProvider = AsyncNotifierProvider<PredictionHistoryNotifier, List<PredictionModel>>(
  () => PredictionHistoryNotifier(),
);
