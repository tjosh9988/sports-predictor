import 'package:flutter_dotenv/flutter_dotenv.dart';

class AppConfig {
  static String get backendUrl => dotenv.get('BACKEND_BASE_URL', fallback: 'https://sports-predictor-1-o34s.onrender.com');
  static String get supabaseUrl => dotenv.get('SUPABASE_URL', fallback: '');
  static String get supabaseAnonKey => dotenv.get('SUPABASE_ANON_KEY', fallback: '');
  
  static const String appVersion = '1.0.0';
  static const String buildNumber = '100';

  static const List<String> supportedSports = [
    'football', 'basketball', 'tennis', 'nfl', 'cricket', 'nhl', 'mlb'
  ];

  static const Map<String, String> bookmakerUrls = {
    'bet365': 'https://www.bet365.com',
    'betway': 'https://www.betway.com',
    '1xbet': 'https://www.1xbet.com',
    'betfair': 'https://www.betfair.com',
  };

  static const String privacyPolicyUrl = 'https://bet-hero.ai/privacy';
  static const String termsOfServiceUrl = 'https://bet-hero.ai/terms';
}
