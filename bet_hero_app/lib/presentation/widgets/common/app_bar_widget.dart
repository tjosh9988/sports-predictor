import 'package:flutter/material.dart';
import '../../../core/theme.dart';


class AppBarWidget extends StatelessWidget implements PreferredSizeWidget {
  final String title;
  final List<Widget>? actions;
  final bool showLogo;

  const AppBarWidget({
    super.key,
    required this.title,
    this.actions,
    this.showLogo = true,
  });

  @override
  Widget build(BuildContext context) {
    return AppBar(
      title: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          if (showLogo) ...[
            const Icon(Icons.bolt, color: AppTheme.primaryGold, size: 28),
            const SizedBox(width: 8),
          ],
          Text(
            title.toUpperCase(),
            style: const TextStyle(
              fontFamily: 'Barlow',
              fontWeight: FontWeight.w900,
              letterSpacing: 1.5,
              fontSize: 20,
            ),
          ),
        ],
      ),
      centerTitle: false,
      actions: [
        ...?actions,
        const SizedBox(width: 8),
      ],
      backgroundColor: AppTheme.background,
      elevation: 0,
    );
  }

  @override
  Size get preferredSize => const Size.fromHeight(kToolbarHeight);
}
