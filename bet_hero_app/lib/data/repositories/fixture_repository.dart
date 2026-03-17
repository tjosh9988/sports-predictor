import 'package:dio/dio.dart';
import 'package:intl/intl.dart';

import '../models/fixture_model.dart';
import '../../services/api_service.dart';

class FixtureRepository {
  final ApiService _api;
  static const int _maxRetries = 2;

  FixtureRepository(this._api);

  Future<List<FixtureModel>> getFixturesBySport(
      String sport, DateTime date) async {
    try {
      final dateStr = DateFormat('yyyy-MM-dd').format(date);

      print('Fetching fixtures: sport=$sport date=$dateStr');

      final response = await _api.dio.get(
        '/sports/$sport/fixtures',
        queryParameters: {'date': dateStr},
      );

      print('Response type: ${response.data.runtimeType}');
      print('Response: ${response.data.toString().substring(0, response.data.toString().length > 200 ? 200 : response.data.toString().length)}');

      List<dynamic> fixturesList = [];

      if (response.data is Map<String, dynamic>) {
        final map = response.data as Map<String, dynamic>;
        fixturesList = map['data'] as List<dynamic>? ?? [];
        print('Found ${fixturesList.length} fixtures in data key');
      } else if (response.data is List) {
        fixturesList = response.data as List<dynamic>;
        print('Response is direct list: ${fixturesList.length}');
      } else {
        print('Unknown response format: ${response.data.runtimeType}');
      }

      if (fixturesList.isEmpty) {
        print('WARNING: Empty fixtures list');
        return [];
      }

      return fixturesList
          .map((f) => FixtureModel.fromJson(f as Map<String, dynamic>))
          .toList();
    } on DioException catch (e) {
      print('DioException: ${e.response?.statusCode} ${e.message}');
      return [];
    } catch (e) {
      print('Error in getFixturesBySport: $e');
      return [];
    }
  }

  Future<FixtureModel?> getFixtureById(String id) async {
    try {
      final response = await _api.get('/predictions/match/$id');
      if (response.data == null || response.data['match'] == null) return null;
      
      final Map<String, dynamic> matchData = Map<String, dynamic>.from(response.data['match']);
      matchData['predictions'] = response.data['predictions'];
      
      return FixtureModel.fromJson(matchData);
    } on DioException catch (e) {
      if (e.response?.statusCode == 404) return null;
      rethrow;
    } catch (e) {
      return null;
    }
  }

  Future<List<FixtureModel>> getUpcomingFixtures() async {
    print('DEBUG: getUpcomingFixtures called');
    int attempts = 0;
    while (attempts <= _maxRetries) {
      try {
        final response = await _api.get('/sports/football/fixtures');
        final List data =
            (response.data is Map ? response.data['data'] : response.data) ??
                [];
        return data.map((json) => FixtureModel.fromJson(json)).toList();
      } on DioException catch (e) {
        if (e.response?.statusCode == 404) return [];

        attempts++;
        if (attempts > _maxRetries) {
          print('DEBUG: getUpcomingFixtures failed after max retries');
          return []; // Return empty instead of rethrowing to stop provider loop
        }
        await Future.delayed(Duration(seconds: 1 * attempts));
      } catch (e) {
        return [];
      }
    }
    return [];
  }

  Future<List<dynamic>> getSports() async {
    try {
      final response = await _api.get('/sports/');
      final List data =
          (response.data is Map ? response.data['data'] : response.data) ?? [];
      return data;
    } catch (e) {
      return [];
    }
  }
}
