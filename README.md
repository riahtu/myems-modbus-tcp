## MyEMS Modbus TCP Service

###Introduction
Collecting data from Modbus TCP slaves


### Prerequisites
pyserial
modbus-tk
mysql.connector

### Installation
Install myems-modbus-tcp
```bash
    $ cd ~
    $ git clone https://github.com/myems/myesm-modbus-tcp.git
    $ sudo cp -R ~/myems-modbus-tcp /myems-modbus-tcp
    $ cd /myems-modbus-tcp
    $ sudo git checkout develop (or the release tag)
```
Open config file and edit database configuration
```bash
    $ sudo nano config.py
```
Setup systemd configure files:
```bash
    $ sudo cp myems-modbus-tcp.service /lib/systemd/system/
```
Next enable the services so they autostart at boot:
```bash
    $ sudo systemctl enable myems-modbus-tcp.service
```
Either reboot, or start the services manually:
```bash
    $ sudo systemctl start myems-modbus-tcp.service
```

### Add Data Sources and Points to database

```bash
INSERT INTO `tbl_data_sources`
(`id`, `name`, `uuid`, `protocol`,  `connection`)
VALUES
(1,'Modbus Meter','e3a6ddcd-88e0-4857-818f-17db42461bd8','modbus-tcp',
 '{\"host\":\"192.168.0.100\",\"port\":502}');
```

```bash
INSERT INTO `tbl_points`
(`id`, `name`, `data_source_id`, `object_type`, `units`, `hi_limit`, `low_limit`, `is_trend`, `address`)
VALUES
(1,'线电压 Vb-c',1,'ANALOG_VALUE','V',690,0,0,
 '{\"format\":\">f\",\"function_code\":3,\"number_of_registers\":2,\"offset\":9,\"slave_id\":1}'),
(2,'有功功率',1,'ENERGY_VALUE','Wh',99999999999,0,1,
 '{\"format\":\">d\",\"function_code\":3,\"number_of_registers\":4,\"offset\":801,\"slave_id\":1}');

```

* Address | format
Functions to convert between Python values and C structs.
Python bytes objects are used to hold the data representing the C struct
and also as format strings (explained below) to describe the layout of data in the C struct.

The optional first format char indicates byte order, size and alignment:
    @: native order, size & alignment (default)
    =: native order, std. size & alignment
    <: little-endian, std. size & alignment
    >: big-endian, std. size & alignment
    !: same as >

The remaining chars indicate types of args and must match exactly;
these can be preceded by a decimal repeat count:
    x: pad byte (no data); c:char; b:signed byte; B:unsigned byte;
    ?: _Bool (requires C99; if not available, char is used instead)
    h:short; H:unsigned short; i:int; I:unsigned int;
    l:long; L:unsigned long; f:float; d:double.

Special cases (preceding decimal count indicates length):
    s:string (array of char); p: pascal string (with count byte).
Special cases (only available in native format):
    n:ssize_t; N:size_t;
    P:an integer type that is wide enough to hold a pointer.

Special case (not in native mode unless 'long long' in platform C):
    q:long long; Q:unsigned long long

Whitespace between formats is ignored.

* Address | function_code
    01 (0x01) Read Coils
    02 (0x02) Read Discrete Inputs
    03 (0x03) Read Holding Registers
    04 (0x04) Read Input Registers
    23 (0x17) Read/Write Multiple registers

* Address | number_of_registers
    The number of registers specified in the Request PDU

* Address | offset
    The starting register address specified in the Request PDU

* Address | slave_id
    The slave ID


### References
  [1]. http://myems.io
  [2]. http://www.modbus.org/tech.php
  [3]. https://github.com/ljean/modbus-tk

