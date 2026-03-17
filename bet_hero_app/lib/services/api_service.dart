import 'package:dio/dio.dart';
import 'package:flutter/foundation.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import '../core/config.dart';

class ApiService {
  late final Dio _dio;
  final SupabaseClient _supabase = Supabase.instance.client;

  Dio get dio => _dio;

  ApiService() {
    _dio = Dio(
      BaseOptions(
        baseUrl: AppConfig.backendUrl,
        connectTimeout: const Duration(seconds: 15),
        receiveTimeout: const Duration(seconds: 15),
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
        },
      ),
    );

    _dio.interceptors.add(InterceptorsWrapper(
      onRequest: (options, handler) async {
        // Attach Supabase JWT Token to every request
        final session = _supabase.auth.currentSession;
        if (session != null) {
          options.headers['Authorization'] = 'Bearer ${session.accessToken}';
        }
        
        if (kDebugMode) {
          print('REQUEST[${options.method}] => PATH: ${options.path}');
        }
        return handler.next(options);
      },
      onResponse: (response, handler) {
        if (kDebugMode) {
          print('RESPONSE[${response.statusCode}] => PATH: ${response.requestOptions.path}');
        }
        return handler.next(response);
      },
      onError: (DioException e, handler) async {
        if (kDebugMode) {
          print('ERROR[${e.response?.statusCode}] => PATH: ${e.requestOptions.path}');
        }

        // Handle 401 Unauthorized (Token Expiration)
        if (e.response?.statusCode == 401) {
          try {
            final session = _supabase.auth.currentSession;
            if (session != null) {
              // Supabase handles refresh automatically, but we can force it or retry
              // For simplicity, we assume auto-refresh and retry once
              final response = await _retry(e.requestOptions);
              return handler.resolve(response);
            }
          } catch (err) {
            return handler.next(e);
          }
        }

        // Global error handling can be added here (e.g., logging)
        return handler.next(e);
      },
    ));
  }

  Future<Response> _retry(RequestOptions requestOptions) {
    final options = Options(
      method: requestOptions.method,
      headers: requestOptions.headers,
    );
    return _dio.request<dynamic>(
      requestOptions.path,
      data: requestOptions.data,
      queryParameters: requestOptions.queryParameters,
      options: options,
    );
  }

  // Helper methods for common operations
  Future<Response> get(String path, {Map<String, dynamic>? queryParameters}) async {
    return await _dio.get(path, queryParameters: queryParameters);
  }

  Future<Response> post(String path, {dynamic data}) async {
    return await _dio.post(path, data: data);
  }

  Future<Response> put(String path, {dynamic data}) async {
    return await _dio.put(path, data: data);
  }

  Future<Response> delete(String path) async {
    return await _dio.delete(path);
  }

  /// Verifies connectivity by calling the /health endpoint.
  Future<void> testConnection() async {
    try {
      final response = await get('/health');
      if (kDebugMode) {
        print('✅ Backend Connection Successful: ${response.data}');
      }
    } catch (e) {
      if (kDebugMode) {
        print('❌ Backend Connection Failed: $e');
      }
    }
  }
}
