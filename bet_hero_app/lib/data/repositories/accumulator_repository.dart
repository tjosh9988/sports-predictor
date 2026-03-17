import 'package:dio/dio.dart';

import '../models/accumulator_model.dart';
import '../../services/api_service.dart';

class AccumulatorRepository {
  final ApiService _api;

  AccumulatorRepository(this._api);

  Future<List<AccumulatorModel>> getTodayAccumulators() async {
    try {
      final response = await _api.dio.get('/predictions/accumulators/today');
      
      List<dynamic> accaList = [];
      if (response.data is Map<String, dynamic>) {
        accaList = response.data['data'] as List? ?? [];
      } else if (response.data is List) {
        accaList = response.data as List;
      }
      
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
      
      List<dynamic> accaList = [];
      if (response.data is Map<String, dynamic>) {
        accaList = response.data['data'] as List? ?? [];
      } else if (response.data is List) {
        accaList = response.data as List;
      }
      
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
      final response = await _api.get(
        '/predictions/history', 
        queryParameters: {'page': page, 'limit': limit},
      );
      final List data = (response.data is Map ? response.data['data'] : response.data) ?? [];
      return data.map((json) => AccumulatorModel.fromJson(json)).toList();
    } on DioException catch (e) {
      if (e.response?.statusCode == 404) return [];
      rethrow;
    } catch (e) {
      return [];
    }
  }
}
