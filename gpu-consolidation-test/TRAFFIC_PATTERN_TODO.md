# Traffic Pattern Enhancement TODO

## Goal
Add production-realistic traffic patterns with natural variance and jitter to better simulate real-world workloads.

## Current State
- **Concurrent** pattern: Fixed constant concurrency
- **Sinusoidal** pattern: Smooth sine wave varying 0 to 2x base concurrency

## Proposed Enhancement: "Noisy Sinusoidal" Pattern

Add a third pattern that combines sinusoidal baseline with realistic production variance:

### Implementation Approach
```python
def get_target_concurrency(self) -> int:
    if self.traffic_pattern == "noisy_sinusoidal":
        # Base sinusoidal wave
        elapsed = time.time() - self.start_time
        period = 60.0
        phase = (elapsed / period) * 2 * math.pi
        base_sine = int(self.base_concurrency + self.base_concurrency * math.sin(phase))
        
        # Add realistic production jitter:
        # 1. Short-term variance (request-level noise)
        noise_amplitude = self.base_concurrency * 0.15  # ±15% variance
        short_term_noise = random.gauss(0, noise_amplitude / 3)
        
        # 2. Occasional micro-spikes (simulates burst traffic)
        if random.random() < 0.05:  # 5% chance per check
            spike = random.randint(1, int(self.base_concurrency * 0.3))
        else:
            spike = 0
        
        target = int(base_sine + short_term_noise + spike)
        return max(0, min(target, self.base_concurrency * 3))  # Cap at 3x
```

### Why This Matters
1. **Realistic testing**: Production traffic is never smooth - it has:
   - Random variance from user behavior
   - Micro-bursts from retry storms, cron jobs, batch operations
   - Natural fluctuations throughout the day

2. **Better flow control validation**: Smooth sinusoidal traffic might hide issues that only appear with realistic variance

3. **More convincing demos**: The jagged, production-like graph (as seen in the UI) looks more credible than perfect sine waves

### Configuration
Add `--traffic-pattern noisy_sinusoidal` option to run_test.py and expose in flow_ui.html presets.

### Next Steps
1. ✅ Get basic sinusoidal pattern working in UI
2. ⬜ Implement noisy_sinusoidal in traffic_generator.py
3. ⬜ Add preset to flow_ui.html
4. ⬜ Test and tune noise parameters for realistic appearance
5. ⬜ Document in demo guide

## Notes
- User feedback (2026-06-27): "i like that the line isnt exactly smooth because thats how real production traffic is"
- Keep noise parameters tunable so demos can adjust realism level
- Consider adding other production patterns (e.g., "daily_cycle", "flash_crowd")
