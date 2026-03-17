import 'package:flutter/material.dart';
import '../../../core/theme.dart';

class StakeCalculator extends StatefulWidget {
  final double totalOdds;

  const StakeCalculator({super.key, required this.totalOdds});

  @override
  State<StakeCalculator> createState() => _StakeCalculatorState();
}

class _StakeCalculatorState extends State<StakeCalculator> {
  final TextEditingController _controller = TextEditingController(text: '10');
  double _return = 0;

  @override
  void initState() {
    super.initState();
    _calculate(text: '10');
  }

  void _calculate({String? text}) {
    final val = double.tryParse(text ?? _controller.text) ?? 0;
    setState(() {
      _return = val * widget.totalOdds;
    });
  }

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: AppTheme.secondaryBackground,
        borderRadius: BorderRadius.circular(12),
        border: Border(
          bottom: BorderSide(
            color: Colors.white.withValues(alpha: 0.05),
            width: 1.0,
          ),
        ),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Text(
            'Potential Return',
            style: TextStyle(color: AppTheme.textSecondary, fontSize: 12),
          ),
          const SizedBox(height: 12),
          Row(
            children: [
              Expanded(
                child: TextField(
                  controller: _controller,
                  keyboardType: TextInputType.number,
                  onChanged: (val) => _calculate(text: val),
                  style: const TextStyle(
                    color: AppTheme.textPrimary,
                    fontWeight: FontWeight.bold,
                  ),
                  decoration: InputDecoration(
                    prefixText: '\$ ',
                    prefixStyle: const TextStyle(color: AppTheme.primaryGold),
                    hintText: 'Stake',
                    isDense: true,
                    fillColor: AppTheme.cardBackground,
                  ),
                ),
              ),
              const SizedBox(width: 16),
              const Icon(Icons.arrow_forward, color: AppTheme.textSecondary, size: 16),
              const SizedBox(width: 16),
              Column(
                crossAxisAlignment: CrossAxisAlignment.end,
                children: [
                  Text(
                    '\$ ${_return.toStringAsFixed(2)}',
                    style: const TextStyle(
                      color: AppTheme.successGreen,
                      fontSize: 18,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                  Text(
                    '+ \$ ${(_return - (double.tryParse(_controller.text) ?? 0)).toStringAsFixed(2)} profit',
                    style: const TextStyle(color: AppTheme.textSecondary, fontSize: 10),
                  ),
                ],
              ),
            ],
          ),
        ],
      ),
    );
  }
}
