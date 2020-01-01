package ipaddr

import (
	"errors"
	"strings"
)

type IPRange struct {
	Lower IPAddr
	Upper IPAddr
}

// TODO(jeb) ip4r's iprange allows a single IPAddr as a valid value,
// and thus not always requiring a pair of IPAddrs

func ParseIPRange(s string, dest *IPRange) error {
	ips := strings.Split(s, "-")
	if len(ips) > 2 {
		// TODO(jeb) not sure if this is the correct way to return an error
		return errors.New("too many IPs in the input string: " + s)
	}

	var lower IPAddr
	var err error
	err = ParseINet(ips[0], &lower)
	if err != nil {
		return err
	}

	var upper IPAddr
	if len(ips) == 1 {
		// TODO(jeb) don't force the upper here, just accept nil/null as upper's value
		upper = lower
	} else {
		err2 := ParseINet(ips[1], &upper)
		if err2 != nil {
			return err2
		}
	}

	*dest = IPRange{lower, upper}
	return nil
}

func (ipRange *IPRange) IsCidr() bool {
	// TODO(jeb) impl me
	return false
}

// Compare two IPAddrs. IPv4-mapped IPv6 addresses are not equal to their IPv4
// mapping. The order of order importance goes Family > Mask > IP-bytes.
func (ipRange *IPRange) Compare(other *IPRange) int {
	cmp := ipRange.Lower.Compare(&other.Lower)
	if cmp != 0 {
		return cmp
	}
	return ipRange.Upper.Compare(&other.Upper)
}

// Equal checks if the family, mask, and IP are equal.
func (ipRange *IPRange) Equal(other *IPRange) bool {
	return ipRange.Lower.Equal(&other.Lower) &&
		ipRange.Upper.Equal(&other.Upper)
}

// ContainsOrEquals determines if one ipAddr is in the same
// subnet as another or the addresses and subnets are equal.
func (ipRange IPRange) ContainsOrEquals(other *IPRange) bool {
	// TODO(jeb) impl me
  //	return ipRange.contains(other) && ipAddr.Mask <= other.Mask
	return false
}

// Contains determines if one ipAddr is in the same
// subnet as another.
func (ipRange IPRange) Contains(other *IPRange) bool {
	// TODO(jeb) impl me
	//return ipAddr.contains(other) && ipAddr.Mask < other.Mask
	return false
}

// ContainedByOrEquals determines if one ipAddr is in the same
// subnet as another or the addresses and subnets are equal.
func (ipRange IPRange) ContainedByOrEquals(other *IPRange) bool {
	// TODO(jeb) impl me
	//return other.contains(&ipAddr) && ipAddr.Mask >= other.Mask
	return false
}

// ContainedBy determines if one ipAddr is in the same
// subnet as another.
func (ipRange IPRange) ContainedBy(other *IPRange) bool {
	// TODO(jeb) impl me
	//return other.contains(&ipAddr) && ipAddr.Mask > other.Mask
	return false
}

// ContainsOrContainedBy determines if one ipAddr is in the same
// subnet as another or vice versa.
func (ipRange IPRange) ContainsOrContainedBy(other *IPRange) bool {
	// TODO(jeb) impl me
	//return ipAddr.contains(other) || other.contains(&ipAddr)
	return false
}

// String will convert the IPRange to the appropriate family formatted string
// representation. In order to retain postgres compatibility we ensure
// IPv4-mapped IPv6 stays in IPv6 format, unlike net.Addr.String().
func (ipRange IPRange) String() string {
	//if ipRange.Lower.Equal(&ipRange.Upper) {
	//	return ipRange.Lower.String()
	//}
	return ipRange.Lower.String() + "-" + ipRange.Upper.String()
}

// ToBuffer appends the IPRange encoding to a buffer and returns the final buffer.
func (ipRange *IPRange) ToBuffer(appendTo []byte) []byte {
	appendTo = ipRange.Lower.ToBuffer(appendTo)
	return ipRange.Upper.ToBuffer(appendTo)
}

// FromBuffer populates an IPRange with data from a byte slice, returning the
// remaining buffer or an error.
func (ipRange *IPRange) FromBuffer(data []byte) ([]byte, error) {
	var lower IPAddr
	//var err error
	data, err := lower.FromBuffer(data)
	if err != nil{
		return data, err
	}
	var upper IPAddr
	data2, err2 := upper.FromBuffer(data)
	if err2 != nil{
		return data2, err2
	}
	ipRange.Lower = lower
	ipRange.Upper = upper
	return data2, nil
}
