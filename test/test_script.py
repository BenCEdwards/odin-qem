from current_measure_test import current_test
from volt_measure_test import voltage_test
from oscillator_test import oscillator_test 
from resistor_test import resistor_test
import sys

resistor_names = ('AUXSAMPLE','AUXRESET','VCM','DACEXTREF','VRESET','VDD_RST','VCTRL')
vc_names = ('VDD0', 'VDD_D18', 'VDD_D25', 'VDD_P18', 'VDD_A18_PLL', 'VDD_D18ADC', 'VDD_D18_PLL', 'VDD_RST', 'VDD_A33', 'VDD_D33', 'VCTRL_NEG', 'VRESET', 'VCTRL_POS')

if __name__ == '__main__':
  base_url = None
  set_url = None
  if len(sys.argv) == 2:
     base_url = sys.argv(2)
     set_url = 'url=' + base_url

  if base_url: clock_tester = oscillator_test(set_url)
  else: clock_tester = oscillator_test()
  results = clock_tester.testClock()
  print 'At Crystal Oscillator, in MHz:'
  for i in range(len(results[0])): 
    print '    expected {:.2f}, measured {:.2f}'.format(results[0][i], results[1][i])

  if base_url: resist_tester = resistor_test(base_url)
  else: resist_tester = resistor_test()
  for name in resistor_names:
    results = resist_tester.testResistor(name)
    print 'At {}, in {}:'.format(name,resist_tester.units[name])
    for i in range(len(results[0])): 
      print '    expected {:.2f}, measured {:.2f}'.format(results[0][i], results[1][i])

  if base_url: volt_tester = voltage_test(base_url)
  else: voltage_test()
  for name in vc_names:
    results = volt_tester.checkVoltage(name)
    if results[0]:
      print 'At {}s register:\n    expected range {:d} to {:d}, measured {:d}'.format(name, results[1][0], results[1][1], results[2])
    else:
      print 'At {}s register:\n    expected {:d}, measured {:d}'.format(name, results[1], results[2])

  if base_url: current_tester = current_test(base_url)
  else: current_tester = current_test()
  for name in vc_names:
    results = current_tester.checkCurrent(name)
    print 'At {}s register:\n    expected {:d}, measured {:d} at default resistance\n    expected {:d}, measured {:d} with added resistor\n'.format(name, results[0][0], results[1][0], results[0][1], results[1][1])