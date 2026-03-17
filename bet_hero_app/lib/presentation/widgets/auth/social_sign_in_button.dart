import 'package:flutter/material.dart';
import '../../../core/theme.dart';

class SocialSignInButton extends StatelessWidget {
  final String label;
  final VoidCallback onPressed;
  final String iconPath;

  const SocialSignInButton({
    super.key,
    required this.label,
    required this.onPressed,
    required this.iconPath,
  });

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      width: double.infinity,
      height: 52,
      child: OutlinedButton(
        onPressed: onPressed,
        style: OutlinedButton.styleFrom(
          side: const BorderSide(color: Colors.white24),
          foregroundColor: AppTheme.textPrimary,
        ),
        child: Row(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            Image.asset(iconPath, height: 24),
            const SizedBox(width: 12),
            Text(label, style: const TextStyle(fontWeight: FontWeight.w600)),
          ],
        ),
      ),
    );
  }
}
