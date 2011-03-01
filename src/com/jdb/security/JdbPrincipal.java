package com.jdb.security;

import java.security.Principal;


public class JdbPrincipal implements Principal, java.io.Serializable 
{
    
    private String name;

    
    public JdbPrincipal(String name) {
	if (name == null)
	    throw new NullPointerException("illegal null input");

	this.name = name;
    }

   
    public String getName() {
	return name;
    }

   
    public String toString() {
	return("SamplePrincipal:  " + name);
    }

   
    public boolean equals(Object o) {
	if (o == null)
	    return false;

        if (this == o)
            return true;
 
        if (!(o instanceof JdbPrincipal))
            return false;
        JdbPrincipal that = (JdbPrincipal)o;

	if (this.getName().equals(that.getName()))
	    return true;
	return false;
    }
 
   
    public int hashCode() {
	return name.hashCode();
    }
}

