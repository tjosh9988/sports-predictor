import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import '../../core/di.dart';

// Stream of auth state changes (exposed from core/di initially, refined here)
final authStateProvider = StreamProvider<AuthState>((ref) {
  final authService = ref.watch(authServiceProvider);
  return authService.authStateChanges;
});

// Provider for the currently logged-in user
final currentUserProvider = Provider<User?>((ref) {
  final authService = ref.watch(authServiceProvider);
  return authService.currentUser;
});

// Provider for session validation status
final isAuthenticatedProvider = Provider<bool>((ref) {
  final user = ref.watch(currentUserProvider);
  return user != null;
});

// Provider for user metadata/preferences
final userPreferencesProvider = Provider<Map<String, dynamic>>((ref) {
  final user = ref.watch(currentUserProvider);
  return user?.userMetadata ?? {};
});
