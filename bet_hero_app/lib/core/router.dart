import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';
import '../presentation/screens/splash_screen.dart';
import '../presentation/screens/onboarding_screen.dart';
import '../presentation/screens/auth/login_screen.dart';
import '../presentation/screens/auth/register_screen.dart';
import '../presentation/screens/main_shell.dart';
import '../presentation/screens/home/dashboard_screen.dart';
import '../presentation/screens/accumulators/accumulators_screen.dart';
import '../presentation/screens/accumulators/accumulator_detail_screen.dart';
import '../presentation/screens/results/results_screen.dart';
import '../presentation/screens/stats/stats_screen.dart';
import '../presentation/screens/fixtures/fixtures_screen.dart';
import '../presentation/screens/fixtures/fixture_detail_screen.dart';
import '../presentation/screens/notifications/notifications_screen.dart';
import '../presentation/screens/settings/settings_screen.dart';

final GlobalKey<NavigatorState> _rootNavigatorKey = GlobalKey<NavigatorState>();
final GlobalKey<NavigatorState> _shellNavigatorKey = GlobalKey<NavigatorState>();

final AppRouter = GoRouter(
  initialLocation: '/splash',
  navigatorKey: _rootNavigatorKey,
  routes: [
    GoRoute(
      path: '/splash',
      builder: (context, state) => const SplashScreen(),
    ),
    GoRoute(
      path: '/onboarding',
      builder: (context, state) => const OnboardingScreen(),
    ),
    GoRoute(
      path: '/auth/login',
      builder: (context, state) => const LoginScreen(),
    ),
    GoRoute(
      path: '/auth/register',
      builder: (context, state) => const RegisterScreen(),
    ),

    ShellRoute(
      navigatorKey: _shellNavigatorKey,
      builder: (context, state, child) => MainShell(child: child),
      routes: [
        GoRoute(
          path: '/home',
          builder: (context, state) => const DashboardScreen(),
        ),
        GoRoute(
          path: '/home/accumulators',
          builder: (context, state) => const AccumulatorsScreen(),
        ),
        GoRoute(
          path: '/home/fixtures',
          builder: (context, state) => const FixturesScreen(),
        ),
        GoRoute(
          path: '/home/results',
          builder: (context, state) => const ResultsScreen(),
        ),
        GoRoute(
          path: '/home/stats',
          builder: (context, state) => const StatsScreen(),
        ),
      ],
    ),

    GoRoute(
      path: '/accumulator/:type',
      builder: (context, state) {
        final type = state.pathParameters['type'] ?? '10odds';
        final indexStr = state.uri.queryParameters['index'] ?? '0';
        final index = int.tryParse(indexStr) ?? 0;
        return AccumulatorDetailScreen(type: type, index: index);
      },
    ),
    GoRoute(
      path: '/fixture/:id',
      builder: (context, state) {
        final id = state.pathParameters['id'] ?? '';
        return FixtureDetailScreen(id: id);
      },
    ),
    GoRoute(
      path: '/notifications',
      builder: (context, state) => const NotificationsScreen(),
    ),
    GoRoute(
      path: '/settings',
      builder: (context, state) => const SettingsScreen(),
    ),
  ],
);
