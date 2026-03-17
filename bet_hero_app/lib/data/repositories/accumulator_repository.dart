import 'package:dio/dio.dart';

import '../models/accumulator_model.dart';
import '../../services/api_service.dart';

class AccumulatorRepository {
  final ApiService _api;

  AccumulatorRepository(this._api);

  List<dynamic> _parseList(dynamic responseData) {
    if (responseData is Map<String, dynamic>) {
      return responseData['data'] as List<dynamic>? ?? [];
    } else if (responseData is List) {
      return responseData as List<dynamic>;
    }
    return [];
  }

  Future<List<AccumulatorModel>> getTodayAccumulators() async {
    try {
      final response = await _api.dio.get('/predictions/accumulators/today');
      final List<dynamic> accaList = _parseList(response.data);
      
      print('Today accumulators: ${accaList.length}');

      return accaList
          .map((a) => AccumulatorModel.fromJson(
              a as Map<String, dynamic>))
          .toList();
    } on DioException catch (e) {
      if (e.response?.statusCode == 404) return [];
      rethrow;
    } catch (e) {
      print('getTodayAccumulators error: $e');
      return [];
    }
  }

  Future<AccumulatorModel?> getAccumulatorByType(AccaType type) async {
    try {
      final response = await _api.dio.get('/predictions/accumulators/${type.value}');
      final List<dynamic> accaList = _parseList(response.data);
      
      print('Accumulators for ${type.value}: ${accaList.length}');
      
      if (accaList.isEmpty) return null;
      
      return AccumulatorModel.fromJson(
          accaList.first as Map<String, dynamic>);
    } on DioException catch (e) {
      if (e.response?.statusCode == 404) return null;
      rethrow;
    } catch (e) {
      print('Accumulator error ${type.value}: $e');
      return null;
    }
  }

  Future<List<AccumulatorModel>> getAccumulatorHistory({
    int page = 1,
    int limit = 20,
  }) async {
    try {
      final response = await _api.dio.get(
        '/results/history', 
        queryParameters: {'page': page, 'limit': limit},
      );
      final List<dynamic> data = _parseList(response.data);
      return data.map((json) => AccumulatorModel.fromJson(json as Map<String, dynamic>)).toList();
    } on DioException catch (e) {
      if (e.response?.statusCode == 404) return [];
      rethrow;
    } catch (e) {
      print('getAccumulatorHistory error: $e');
      return [];
    }
  }
}
