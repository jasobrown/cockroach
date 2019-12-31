package ipaddr

import (
	"errors"
	"strings"
)

type IPRange struct {
	Lower IPAddr
	Upper IPAddr
}

func ParseIPRange(s string, dest *IPRange) error {
	ips := strings.Split(s, "-")
	if len(ips) != 2 {
		// TODO(jeb) not sure if this is the correct way to return an error
		return errors.New("must have two IPs in the input string, separated by a '-'")
	}

	var lower IPAddr
	var err error
	err = ParseINet(ips[0], &lower)
	if err != nil {
		return err
	}

	var upper IPAddr
	err2 := ParseINet(ips[1], &upper)
	if err2 != nil {
		return err2
	}

	*dest = IPRange{lower, upper}
	return nil
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

// String will convert the IPRange to the appropriate family formatted string
// representation. In order to retain postgres compatibility we ensure
// IPv4-mapped IPv6 stays in IPv6 format, unlike net.Addr.String().
func (ipRange IPRange) String() string {
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
