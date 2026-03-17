import 'package:dio/dio.dart';

import '../models/prediction_model.dart';
import '../models/performance_model.dart';
import '../../services/api_service.dart';

class ResultsRepository {
  final ApiService _api;

  ResultsRepository(this._api);

  Future<List<PredictionModel>> getPredictionHistory({int page = 1}) async {
    try {
      final response = await _api.get(
        '/results/history',
        queryParameters: {'page': page, 'limit': 50},
      );
      final List data = (response.data is Map ? response.data['data'] : response.data) ?? [];
      return data.map((json) => PredictionModel.fromJson(json)).toList();
    } on DioException catch (e) {
      if (e.response?.statusCode == 404) return [];
      rethrow;
    } catch (e) {
      return [];
    }
  }

  Future<PerformanceModel> getPerformanceStats() async {
    try {
      final response = await _api.get('/results/performance');
      final List data = response.data ?? [];
      if (data.isEmpty) {
        return PerformanceModel.empty();
      }
      return PerformanceModel.fromJson(data.first);
    } on DioException catch (e) {
      if (e.response?.statusCode == 404) return PerformanceModel.empty();
      rethrow;
    } catch (e) {
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
