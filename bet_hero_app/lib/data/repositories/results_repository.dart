import 'package:dio/dio.dart';

import '../models/accumulator_model.dart';
import '../models/prediction_model.dart';
import '../models/performance_model.dart';
import '../../services/api_service.dart';

class ResultsRepository {
  final ApiService _api;

  ResultsRepository(this._api);

  List<dynamic> _parseList(dynamic responseData) {
    if (responseData is Map<String, dynamic>) {
      return responseData['data'] as List<dynamic>? ?? [];
    } else if (responseData is List) {
      return responseData as List<dynamic>;
    }
    return [];
  }

  Future<List<AccumulatorModel>> getAccumulatorHistory({int page = 1}) async {
    try {
      final response = await _api.dio.get(
        '/results/history',
        queryParameters: {'page': page, 'limit': 50},
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

  Future<PerformanceModel> getPerformanceStats() async {
    try {
      final response = await _api.dio.get('/results/performance');
      if (response.data == null) {
        return PerformanceModel.empty();
      }
      return PerformanceModel.fromJson(response.data as Map<String, dynamic>);
    } on DioException catch (e) {
      if (e.response?.statusCode == 404) return PerformanceModel.empty();
      rethrow;
    } catch (e) {
      print('getPerformanceStats error: $e');
      return PerformanceModel.empty();
    }
  }

  Future<Map<String, double>> getAccuracyBySport(String sport) async {
    try {
      final response = await _api.get('/results/accuracy/$sport');
      return Map<String, double>.from(response.data ?? {});
    } on DioException catch (e) {
      if (e.response?.statusCode == 404) return {};
      rethrow;
    } catch (e) {
      return {};
    }
  }

  Future<double> getRoiByTimeframe(String timeframe) async {
    try {
      final response = await _api.get('/results/roi/$timeframe');
      if (response.data == null) return 0.0;
      return (response.data['overall_roi'] as num? ?? 0.0).toDouble();
    } on DioException catch (e) {
      if (e.response?.statusCode == 404) return 0.0;
      rethrow;
    } catch (e) {
      return 0.0;
    }
  }
}
