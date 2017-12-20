import sys
import signal
import logging
import math

from i2c_device import I2CDevice, I2CException
from i2c_container import I2CContainer

from tca9548 import TCA9548
from mcp23008 import MCP23008
from tpl0102 import TPL0102
from si570 import SI570
from ad7998 import AD7998

try :
    from logger.qem_logger import qemLogger
except:
    logger_imported = False
else:
    logger_imported = True

class Backplane(I2CContainer):
    CURRENT_RESISTANCE = [2.5, 1, 1, 1, 10, 1, 10, 1, 1, 1, 10, 1, 10]

    def __init__(self):
        #Set up I2C devices
        signal.signal(signal.SIGALRM, self.connect_handler)
        signal.alarm(6)
        try:
            self.tca = TCA9548(0x70, busnum=1)

            self.tpl0102 = []
            for i in range(5):
                self.tpl0102.append(self.tca.attach_device(0, TPL0102, 0x50 + i, busnum=1))
            for i in range(5):
                self.tpl0102[i].set_non_volatile(True)

            self.si570 = self.tca.attach_device(1, SI570, 0x5d, 'SI570', busnum=1)
            self.si570.set_frequency(20) #Default to 20MHz

            self.ad7998 = []
            for i in range(4):
                self.ad7998.append(self.tca.attach_device(2, AD7998, 0x21 + i, busnum=1))

            self.mcp23008 = []
            self.mcp23008.append(self.tca.attach_device(3, MCP23008, 0x20, busnum=1))
            self.mcp23008.append(self.tca.attach_device(3, MCP23008, 0x21, busnum=1))
            for i in range(8):
                self.mcp23008[0].setup(i, MCP23008.IN)
            self.mcp23008[1].output(0, MCP23008.HIGH)
            self.mcp23008[1].setup(0, MCP23008.OUT)

            #Resistor readings
            self.resistors_raw = [
                self.tpl0102[0].get_wiper(0),
                self.tpl0102[0].get_wiper(1),
                self.tpl0102[1].get_wiper(0),
                self.tpl0102[2].get_wiper(0),
                self.tpl0102[2].get_wiper(1),
                self.tpl0102[3].get_wiper(0),
                self.tpl0102[4].get_wiper(0)
]
            self.resistors = [
                3.3 * (390 * self.resistors_raw[0]) / (390 * self.resistors_raw[0] + 32000),
                3.3 * (390 * self.resistors_raw[1]) / (390 * self.resistors_raw[1] + 32000),
                400.0 * (390 * self.resistors_raw[2]) / (390 * self.resistors_raw[2] + 294000),
                0.0001 * (17800 + (18200 * (390 * self.resistors_raw[3])) / (18200 + (390 * self.resistors_raw[3]))),
                0.0001 * (49900 * (390 * self.resistors_raw[4])) / (49900 + (390 * self.resistors_raw[4])),
                -3.775 + (1.225/22600 + .35*.000001) * (390 * self.resistors_raw[5] + 32400),
                3.3 * (390 * self.resistors_raw[6]) / (390 * self.resistors_raw[6] + 32000),
]
        except Exception, exc:
            if exc == 13:
                logging.error("I2C Communications not enabled for user. Try 'su -;chmod 666 /dev/i2c-1'")
            else:
                logging.error(exc)
            sys.exit(1)
        finally:
            signal.alarm(0)
            if logger_imported:
                self.logger_state = u"0"
                self.logger = None
            else:
                self.logger_state = u"N/A"

        #Placeholders for sensor readings
        self.voltages = [0.0] * 13
        self.voltages_raw = [0.0] * 13
        self.currents = [0.0] * 13
        self.currents_raw = [0.0] * 13
        self.power_good = [False] * 8
        self.psu_enabled = True
        self.clock_freq = 20.0
        self.resistor_volatile = False
        self.temperature = 0

        self.voltChannelLookup = ((0,2,3,4,5,6,7),(0,2,4,5,6,7))
        self.updates_needed = 1
        self.set_sensors_enable(False)

    def connect_handler(self, signum, frame):
        raise Exception("Timeout on I2C connection, Shutting Down")

    def timeout_handler(self, signum, frame):
        raise Exception("Timeout on I2C communication")

    def poll_all_sensors(self):

        if not (self.sensors_enabled or (self.updates_needed > 0)) : return

        signal.signal(signal.SIGALRM, self.timeout_handler)
        signal.alarm(1)
        try:
            #Currents
            for i in range(7):
                j = self.voltChannelLookup[0][i]
                self.currents_raw[i] = (self.ad7998[0].read_input_raw(j) & 0xfff)
                self.currents[i] = self.currents_raw[i] / self.CURRENT_RESISTANCE[i] * 5000 / 4095.0

            for i in range(6):
                j = self.voltChannelLookup[1][i]
                self.currents_raw[i + 7] = (self.ad7998[2].read_input_raw(j) & 0xfff)
                self.currents[i + 7] = self.currents_raw[i + 7] / self.CURRENT_RESISTANCE[i + 7] * 5000 / 4095.0

            #Voltages
            for i in range(7):
                j = self.voltChannelLookup[0][i]
                self.voltages_raw[i] = self.ad7998[1].read_input_raw(j) & 0xfff
                self.voltages[i] = self.voltages_raw[i] * 3 / 4095.0
            for i in range(6):
                j = self.voltChannelLookup[1][i]
                self.voltages_raw[i + 7] = self.ad7998[3].read_input_raw(j) & 0xfff
                self.voltages[i + 7] = self.voltages_raw[i + 7] * 5 / 4095.0
            self.voltages[10] *= -1

            #first calculate the voltage from the register
            temp_volt = (self.ad7998[3].read_input_raw(3) & 0xfff) * 5.0 / 4095.0
            #then calculate the natural log of the calculated resistance (5V potential divider with 15K resistor) divided by the resistance at 25 degrees celcius(10K)
            ln_x = math.log(1.5*temp_volt/(5.0-temp_volt))
            #then calulate the temperature using formula from the data sheet of thermistor NTCALUG03A103G and convert from Kelvin
            self.temperature = 1.0/(0.00335402 + ln_x*(0.00025624 + ln_x*(0.00000260597 + ln_x*0.0000000632926)))-273.15

            #Power good monitors
            self.power_good = self.mcp23008[0].input_pins([0,1,2,3,4,5,6,7,8])
        except Exception, exc:
            logging.warning(exc)
        finally:
            signal.alarm(0)
            if self.logger_state == u"2": self.update_log()
            if self.updates_needed > 0: self.updates_needed -= 1

    def set_resistor_value(self, resistor, value):
        if resistor == 0:
            self.resistors_raw[resistor] = int(0.5+(32000/3.3)*value/(390-390*value/3.3))
            self.tpl0102[0].set_wiper(0, self.resistors_raw[resistor])
        elif resistor == 1:
            self.resistors_raw[resistor] = int(0.5+(32000/3.3)*value/(390-390*value/3.3))
            self.tpl0102[0].set_wiper(1, self.resistors_raw[resistor])
        elif resistor == 2:
            self.resistors_raw[resistor] = int(0.5+(294000/400)*value/(390-390*value/400))
            self.tpl0102[1].set_wiper(0, self.resistors_raw[resistor])
        elif resistor == 3:
            self.resistors_raw[resistor] = int(0.5+(18200/0.0001)*(value-1.78)/(390*18200-390*(value-1.78)/0.0001))
            self.tpl0102[2].set_wiper(0, self.resistors_raw[resistor])
        elif resistor == 4:
            self.resistors_raw[resistor] = int(0.5+(49900/0.0001)*value/(390*49900-390*value/0.0001))
            self.tpl0102[2].set_wiper(1, self.resistors_raw[resistor])
        elif resistor == 5:
            self.resistors_raw[resistor] = int(0.5+((value+3.775)/(1.225/22600+.35*.000001)-32400)/390)
            self.tpl0102[3].set_wiper(1, self.resistors_raw[resistor])
        elif resistor == 6:
            self.resistors_raw[resistor] = int(0.5+(32000/3.3)*value/(390-390*value/3.3))
            self.tpl0102[4].set_wiper(0, self.resistors_raw[resistor])
        self.resistors[resistor] = value
        if not self.sensors_enabled: self.updates_needed = 1

    def set_resistor_value_raw(self, resistor, value):
        if resistor == 0:
            self.tpl0102[0].set_wiper(0, value)
            self.resistors[resistor] = 3.3 * (390 * value) / (390 * value + 32000)
        elif resistor == 1:
            self.tpl0102[0].set_wiper(1, value)
            self.resistors[resistor] = 3.3 * (390 * value) / (390 * value + 32000)
        elif resistor == 2:
            self.tpl0102[1].set_wiper(0, value)
            self.resistors[resistor] = 400 * (390 * value) / (390 * value + 294000)
        elif resistor == 3:
            self.tpl0102[2].set_wiper(0, value)
            self.resistors[resistor] = 0.0001 * (17800 + (18200 * (390 * value)) / (18200 + (390 * value)))
        elif resistor == 4:
            self.tpl0102[2].set_wiper(1, value)
            self.resistors[resistor] = 0.0001 * (49900 * (390 * value)) / (49900 + (390 * value))
        elif resistor == 5:
            self.tpl0102[3].set_wiper(0, value)
            self.resistors[resistor] = -3.775 + (1.225/22600 + .35*.000001) * (390 * value + 32400)
        elif resistor == 6:
            self.tpl0102[4].set_wiper(0, value)
            self.resistors[resistor] = 3.3 * (390 * value) / (390 * value + 32000)
        self.resistors_raw[resistor] = value
        if not self.sensors_enabled: self.updates_needed = 1

    def get_resistor_value(self, resistor):
        return self.resistors[resistor]

    def get_resistor_value_raw(self, resistor):
        return self.resistors_raw[resistor]

    def get_resistor_name(self, resistor):
        return ["AUXRESET", "VCM", "DACEXTREF", "VDD_RST", "VRESET", "VCTRL", "AUXSAMPLE"][resistor]

    def get_resistor_units(self, resistor):
        return ["V", "V", "uA", "V", "V", "V", "V"][resistor]

    def get_resistor_min(self, resistor):
        return [0, 0, 0, 0, 0, -2.02, 0][resistor]

    def get_resistor_max(self, resistor):
        return [2.474, 2.474, 99.98, 3.3, 3.3, 3.3, 2.474][resistor]

    def get_resistor_volatile(self):
        return self.resistor_volatile

    def set_resistor_volatile(self, value):
        for i in range(5):
            self.tpl0102[i].set_non_volatile(not value)
        self.resistor_volatile = value

    def get_power_good(self, i):
        return self.power_good[i]

    def get_clock_frequency(self):
        return self.clock_freq

    def set_clock_frequency(self, freq):
        self.clock_freq = freq + 0.0
        self.si570.set_frequency(freq)

    def get_psu_enable(self):
        return self.psu_enabled

    def set_psu_enable(self, value):
        self.psu_enabled = value
        self.mcp23008[1].output(0, MCP23008.HIGH if value else MCP23008.LOW)
        if not self.sensors_enabled: self.updates_needed = 3

    def get_sensors_enable(self):
        return self.sensors_enabled

    def set_sensors_enable(self, value):
        self.sensors_enabled = value

    def get_update(self):
        return (self.sensors_enabled or self.updates_needed > 0)

    def set_update(self, value):
        if value and not self.sensors_enabled: self.updates_needed = 1

    def get_logger_state(self):
        if self.logger_state == u"1":
            if not self.logger.do_update:
                self.logger_state =  u"0"
                self.logger = None
            elif self.logger.setup_done:
                self.logger_state = u"2"
        return self.logger_state

    def set_logger_state(self, locations):
        if self.logger_state == u"0":
            pscu_host,db_location = locations.split(";")
            self.start_log(pscu_host,db_location)
            self.logger_state = u"1"
        elif self.logger_state in {u"1", u"2"}:
            self.logger_state = u"0"
            self.logger = None

    def start_log(self,pscu_host,db_location):
        self.logger = qemLogger(pscu_host,db_location)

    def update_log(self):
        log_power_data = {}
        for i in range(8): log_power_data[i+1] = self.power_good[i]
        log_supply_data = {}
        for i in range(13):
            log_supply_data[i] = {
                "voltage": self.voltages[i],
                "voltage_register": self.voltages_raw[i],
                "current": self.currents[i],
                "current_register": self.currents_raw[i],
            }
        log_resistor_data = {}
        for i in range(7):
            log_resistor_data[i] = {
                'resistance': self.resistors[i],
                'register': self.resistors_raw[i],
            }
        log_data = {
            'sensors_enabled': self.sensors_enabled,
            'psu_enabled': self.psu_enabled,
            'clock': self.clock_freq,
            'power_good': log_power_data,
            'current_voltage': log_supply_data,
            'resistors': log_resistor_data,
        }
        self.logger.do_update(log_data)

    def set_reset(self, value):
        self.mcp23008[1].setup(0, MCP23008.OUT)
        for i in range(5):
            self.tpl0102[i].set_non_volatile(False)
        self.resistor_volatile = True
        self.set_clock_frequency(20)
        self.resistors_raw = [
            self.tpl0102[0].get_wiper(0,True),
            self.tpl0102[0].get_wiper(1,True),
            self.tpl0102[1].get_wiper(0,True),
            self.tpl0102[2].get_wiper(0,True),
            self.tpl0102[2].get_wiper(1,True),
            self.tpl0102[3].get_wiper(0,True),
            self.tpl0102[4].get_wiper(0,True)
]
        self.resistors = [
            3.3 * (390 * self.resistors_raw[0]) / (390 * self.resistors_raw[0] + 32000),
            3.3 * (390 * self.resistors_raw[1]) / (390 * self.resistors_raw[1] + 32000),
            400 * (390 * self.resistors_raw[2]) / (390 * self.resistors_raw[2] + 294000),
            0.0001 * (17800 + (18200 * (390 * self.resistors_raw[3])) / (18200 + (390 * self.resistors_raw[3]))),
            0.0001 * (49900 * (390 * self.resistors_raw[4])) / (49900 + (390 * self.resistors_raw[4])),
            -3.775 + (1.225/22600 + .35*.000001) * (390 * self.resistors_raw[5] + 32400),
            3.3 * (390 * self.resistors_raw[6]) / (390 * self.resistors_raw[6] + 32000),
]
        self.set_psu_enable(True)

        if self.logger_state != u"N/A":
            self.logger = None
            self.logger_state = u"0"

    def get_temp(self):
        return self.temperature

    def get_current(self, i):
        return self.currents[i]

    def get_current_raw(self, i):
        return self.currents_raw[i]

    def get_voltage(self, i):
        return self.voltages[i]

    def get_voltage_raw(self, i):
        return self.voltages_raw[i]

    def get_adc_name(self, i):
        return ["VDDO", "VDD_D18", "VDD_D25", "VDD_P18",  "VDD_A18_PLL",  "VDD_D18ADC",
               "VDD_D18_PLL", "VDD_RST", "VDD_A33", "VDD_D33", "VCTRL_NEG", "VRESET",
               "VCTRL_POS"][i]
