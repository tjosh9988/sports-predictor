import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:flutter_riverpod/legacy.dart';
import '../../core/di.dart';
import '../../data/models/accumulator_model.dart';

// Provider for today's accumulators
final todayAccumulatorsProvider = FutureProvider<List<AccumulatorModel>>((ref) async {
  final repo = ref.watch(accumulatorRepositoryProvider);
  try {
    return await repo.getTodayAccumulators();
  } catch (e) {
    return []; // Return empty list on error
  }
});

// Provider for accumulators filtered by type
final accumulatorByTypeProvider = FutureProvider.family<AccumulatorModel?, AccaType>((ref, type) async {
  final repo = ref.watch(accumulatorRepositoryProvider);
  return await repo.getAccumulatorByType(type);
});

// State provider for current selected filter type
final selectedAccaTypeProvider = StateProvider<AccaType>((ref) => AccaType.ten);

// AsyncNotifier for paginated accumulator history
class AccumulatorHistoryNotifier extends AsyncNotifier<List<AccumulatorModel>> {
  int _currentPage = 1;
  bool _hasMore = true;

  @override
  Future<List<AccumulatorModel>> build() async {
    _currentPage = 1;
    _hasMore = true;
    return _fetchPage(1);
  }

  Future<List<AccumulatorModel>> _fetchPage(int page) async {
    final repo = ref.read(accumulatorRepositoryProvider);
    return await repo.getAccumulatorHistory(page: page);
  }

  Future<void> fetchNextPage() async {
    if (!_hasMore || state.isLoading) return;
    
    final currentList = state.value ?? [];
    _currentPage++;
    
    final nextPage = await _fetchPage(_currentPage);
    if (nextPage.isEmpty) {
      _hasMore = false;
    } else {
      state = AsyncData([...currentList, ...nextPage]);
    }
  }
}

final accumulatorHistoryProvider = AsyncNotifierProvider<AccumulatorHistoryNotifier, List<AccumulatorModel>>(
  () => AccumulatorHistoryNotifier(),
);
