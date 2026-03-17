import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:go_router/go_router.dart';
import 'package:lottie/lottie.dart';
import '../providers/auth_provider.dart';
import '../../core/theme.dart';

class SplashScreen extends ConsumerStatefulWidget {
  const SplashScreen({super.key});

  @override
  ConsumerState<SplashScreen> createState() => _SplashScreenState();
}

class _SplashScreenState extends ConsumerState<SplashScreen> {
  @override
  void initState() {
    super.initState();
    _handleNavigation();
  }

  Future<void> _handleNavigation() async {
    // Wait for the pulse animation/minimum splash time
    await Future.delayed(const Duration(milliseconds: 2500));
    
    if (!mounted) return;

    final authState = ref.read(authStateProvider).value;
    final user = ref.read(currentUserProvider);

    if (user != null) {
      context.go('/home');
    } else {
      context.go('/onboarding');
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: Container(
        width: double.infinity,
        decoration: const BoxDecoration(
          gradient: LinearGradient(
            begin: Alignment.topCenter,
            end: Alignment.bottomCenter,
            colors: [AppTheme.background, AppTheme.secondaryBackground],
          ),
        ),
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            // Placeholder for Lottie pulse animation
            // In a real app, you'd add the .json asset to assets/animations/
            Hero(
              tag: 'logo',
              child: Container(
                width: 120,
                height: 120,
                decoration: BoxDecoration(
                  color: AppTheme.primaryGold.withOpacity(0.1),
                  shape: BoxShape.circle,
                ),
                child: const Icon(
                  Icons.auto_graph,
                  color: AppTheme.primaryGold,
                  size: 60,
                ),
              ),
            ),
            const SizedBox(height: 24),
            Text(
              'BET HERO',
              style: Theme.of(context).textTheme.displaySmall?.copyWith(
                    letterSpacing: 4,
                    color: AppTheme.primaryGold,
                  ),
            ),
            const SizedBox(height: 8),
            const Text(
              'AI PREDICTIONS',
              style: TextStyle(
                color: AppTheme.textSecondary,
                letterSpacing: 2,
                fontSize: 12,
              ),
            ),
          ],
        ),
      ),
    );
  }
}
