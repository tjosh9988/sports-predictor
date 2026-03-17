import 'package:dio/dio.dart';
import '../../services/api_service.dart';

class UserRepository {
  final ApiService _api;

  UserRepository(this._api);

  Future<Map<String, dynamic>> getPreferences() async {
    try {
      final response = await _api.get('/users/preferences');
      return response.data ?? {};
    } on DioException catch (e) {
      if (e.response?.statusCode == 404) return {};
      rethrow;
    } catch (e) {
      return {};
    }
  }

  Future<bool> updatePreferences(Map<String, dynamic> preferences) async {
    try {
      await _api.put('/users/preferences', data: preferences);
      return true;
    } catch (e) {
      return false;
    }
  }
}
