import 'package:supabase_flutter/supabase_flutter.dart';

class AuthService {
  final SupabaseClient _supabase = Supabase.instance.client;

  // Stream of auth state changes
  Stream<AuthState> get authStateChanges => _supabase.auth.onAuthStateChange;

  // Get current user
  User? get currentUser => _supabase.auth.currentUser;

  // Check if logged in
  bool get isAuthenticated => currentUser != null;

  // Email/Password Registration
  Future<AuthResponse> signUp({
    required String email,
    required String password,
  }) async {
    return await _supabase.auth.signUp(
      email: email,
      password: password,
    );
  }

  // Email/Password Sign In
  Future<AuthResponse> signIn({
    required String email,
    required String password,
  }) async {
    return await _supabase.auth.signInWithPassword(
      email: email,
      password: password,
    );
  }

  // Google Sign In (OAuth)
  // Note: Requires platform-specific configuration for callback URLs
  Future<bool> signInWithGoogle() async {
    return await _supabase.auth.signInWithOAuth(
      OAuthProvider.google,
      redirectTo: 'com.bethero.app://login-callback/',
    );
  }

  // Sign Out
  Future<void> signOut() async {
    await _supabase.auth.signOut();
  }

  // Recover Password
  Future<void> resetPassword(String email) async {
    await _supabase.auth.resetPasswordForEmail(email);
  }

  // Update User Metadata (Preferences)
  Future<UserResponse> updateMetadata(Map<String, dynamic> data) async {
    return await _supabase.auth.updateUser(
      UserAttributes(data: data),
    );
  }

  // Get JWT Token for manual use
  String? get currentToken => _supabase.auth.currentSession?.accessToken;
}
